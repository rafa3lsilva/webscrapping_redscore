import pandas as pd
import data as dt
from datetime import date, timedelta, datetime
import ligas_config as cfg
import os
import logging
import sqlite3
import random
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from auth_redscore import REDSCORE_USER, REDSCORE_PASS
from login_redscore import login_redscore
import requests
from urllib.parse import urljoin

# ================================
# CONFIGURÁVEL
# ================================
MAX_WORKERS_FASE2 = 10             # número de threads para fase 2 (requests)
REQUEST_TIMEOUT = 20               # timeout para requests
VACUUM_SIZE_THRESHOLD_MB = 50      # força VACUUM se DB > isto (MB)
VACUUM_DAY_IS_SUNDAY = True        # ou False se preferir apenas pelo tamanho
# ================================


# ================================
# Configuração Inicial / Logging
# ================================
dia = date.today() + timedelta(days=1)
NOME_DB = "dados.db"

logging.basicConfig(
    filename="coletor.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger(__name__)
log.info("Coletor iniciado")


# ================================
# DB utils
# ================================
def inicializar_banco(nome_db=NOME_DB):
    conn = sqlite3.connect(nome_db)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jogos (
        Data TEXT, Home TEXT, Away TEXT, Liga TEXT, H_Gols_FT INTEGER, A_Gols_FT INTEGER,
        H_Gols_HT INTEGER, A_Gols_HT INTEGER, H_Chute INTEGER, A_Chute INTEGER,
        H_Chute_Gol INTEGER, A_Chute_Gol INTEGER, H_Ataques INTEGER, A_Ataques INTEGER,
        H_Escanteios INTEGER, A_Escanteios INTEGER, Odd_H REAL, Odd_D REAL, Odd_A REAL,
        PRIMARY KEY (Data, Home, Away)
    )""")
    conn.commit()
    conn.close()


def salvar_no_banco(df, nome_db=NOME_DB):
    if df.empty:
        return
    conn = sqlite3.connect(nome_db)
    df.to_sql('jogos', conn, if_exists='append', index=False)
    conn.close()
    log.info(f"Dados salvos/atualizados na tabela 'jogos' ({len(df)} linhas).")


def carregar_jogos_existentes(nome_db=NOME_DB):
    if not os.path.exists(nome_db):
        return set()
    conn = sqlite3.connect(nome_db)
    jogos = {tuple(row) for row in conn.cursor().execute(
        "SELECT Data, Home, Away FROM jogos")}
    conn.close()
    return jogos


def exportar_para_csv(nome_db=NOME_DB, nome_csv="dados_redscore.csv"):
    conn = sqlite3.connect(nome_db)
    df = pd.read_sql_query("SELECT * FROM jogos", conn)
    conn.close()
    df.to_csv(nome_csv, index=False)
    print(f"✅ Histórico completo exportado para {nome_csv} ({len(df)} linhas)")
    log.info(f"Exportado histórico para {nome_csv} ({len(df)} linhas)")


def exportar_jogos_amanha_para_csv(lista_de_jogos, nome_csv=f"jogos_do_dia/Jogos_do_Dia_RedScore_{dia}.csv"):
    if not lista_de_jogos:
        print("Nenhuma agenda de jogos de amanhã para exportar.")
        return
    df = pd.DataFrame(lista_de_jogos)
    os.makedirs(os.path.dirname(nome_csv), exist_ok=True)
    df.to_csv(nome_csv, index=False, encoding='utf-8')
    print(f"✅ Agenda exportada para {nome_csv} ({len(df)} linhas)")
    log.info("Exportada agenda para %s (%d linhas).", nome_csv, len(df))


# ================================
# VACUUM policy
# ================================
def maybe_vacuum_db(nome_db=NOME_DB):
    try:
        size_mb = os.path.getsize(nome_db) / (1024 * 1024)
        today_is_sunday = datetime.today().weekday() == 6
        if size_mb >= VACUUM_SIZE_THRESHOLD_MB or (VACUUM_DAY_IS_SUNDAY and today_is_sunday):
            log.info(f"[DB] Executando VACUUM (tamanho={size_mb:.1f} MB).")
            conn = sqlite3.connect(nome_db)
            conn.execute("VACUUM;")
            conn.close()
            log.info("[DB] VACUUM concluído.")
    except Exception as e:
        log.warning(f"[DB] Não foi possível executar VACUUM: {e}")


# ================================
# Helpers para Fase 2 (requests + cookies)
# ================================
def build_requests_session_from_selenium(driver):
    """
    Extrai cookies do Selenium e cria uma requests.Session com esses cookies.
    Útil para evitar abrir muitos drivers.
    """
    session = requests.Session()
    # Defina um user-agent parecido com o do browser para reduzir bloqueios
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    })
    try:
        for c in driver.get_cookies():
            cookie_dict = {'domain': c.get('domain'), 'name': c.get('name'), 'value': c.get('value'),
                           'path': c.get('path', '/')}
            # requests expects cookie without domain for set-cookie via session.cookies.set
            session.cookies.set(
                cookie_dict['name'], cookie_dict['value'], path=cookie_dict['path'])
    except Exception as e:
        log.warning(f"[F2] Não foi possível extrair cookies do Selenium: {e}")
    return session


def fetch_match_links_by_requests(session, match_url):
    """
    Tenta buscar os links das equipas via HTTP (requests). Retorna (home_link, away_link) ou (None, None).
    Se o HTML não contiver os anchors esperados, retorna (None, None) para identificar fallback.
    """
    try:
        resp = session.get(match_url, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200 or not resp.text:
            return None, None
        # usando BeautifulSoup do data.py (alias)
        soup = dt.BeautifulSoup(resp.text, 'html.parser')
        # tenta os seletores robustos
        anchors = soup.select(
            "div.match-detail__teams a, div.match-detail__name a, div.match-detail__team a")
        if len(anchors) >= 2:
            home = urljoin("https://redscores.com", anchors[0].get('href'))
            away = urljoin("https://redscores.com", anchors[1].get('href'))
            return home, away
        return None, None
    except Exception as e:
        return None, None


# ================================
# Rotina Principal Otimizada
# ================================
def rotina_diaria_noturna():
    inicializar_banco()
    log.info("--- Rotina diária iniciada ---")
    start_global = time.time()

    driver = None
    try:
        print("--- Fase 0: Autenticando no RedScore ---")
        driver = login_redscore(REDSCORE_USER, REDSCORE_PASS)

        # Fase 1: agenda
        print("\n--- Fase 1: Coletando agenda de amanhã ---")
        t1 = time.time()
        jogos_amanha = dt.raspar_jogos_de_amanha(driver, cfg.LIGAS_PERMITIDAS)
        t2 = time.time()
        log.info(f"[TEMPO] Fase 1 concluída em {(t2 - t1):.2f}s")

        if not jogos_amanha:
            print("Nenhum jogo encontrado. Rotina concluída.")
            log.info("Nenhum jogo encontrado para amanhã.")
            return

        exportar_jogos_amanha_para_csv(jogos_amanha)

        # Fase 2: obter links das equipas (paralelo via requests + cookies)
        print(
            f"\n--- Fase 2: Obtendo links das equipas de {len(jogos_amanha)} confrontos ---")
        t1 = time.time()

        # preparar session com cookies do Selenium
        session = build_requests_session_from_selenium(driver)
        equipas_a_visitar = {}
        erros_confronto = []
        faltou_fallback = []

        # Função worker para ThreadPool
        def worker_fetch(jogo):
            url = jogo['link_confronto']
            try:
                # 1) tenta requests
                home, away = fetch_match_links_by_requests(session, url)
                if home and away:
                    return ("OK", home, away, jogo['liga'])
                # 2) sinaliza fallback para selenium sequencial
                return ("FALLBACK", url, jogo['liga'])
            except Exception as e:
                return ("ERROR", url, str(e))

        # dispara os workers
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_FASE2) as exc:
            futures = {exc.submit(worker_fetch, j): j for j in jogos_amanha}
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Verificando Confrontos"):
                try:
                    res = fut.result()
                except Exception as e:
                    log.error(f"[F2] Future result error: {e}")
                    continue

                if res[0] == "OK":
                    _, home, away, liga = res
                    equipas_a_visitar[home] = liga
                    equipas_a_visitar[away] = liga
                elif res[0] == "FALLBACK":
                    _, url, liga = res
                    faltou_fallback.append((url, liga))
                else:
                    _, url, err = res
                    erros_confronto.append((url, err))

        # Se houver fallbacks, processe sequencialmente com Selenium (mais lento, mas robusto)
        if faltou_fallback:
            log.info(
                f"[F2] {len(faltou_fallback)} confrontos requerem fallback com Selenium (sequencial).")
            for url, liga in tqdm(faltou_fallback, desc="Fallback Selenium (confrontos)"):
                try:
                    home, away = dt.obter_links_equipes_confronto(
                        driver, url)  # já tem retry no data.py
                    if home and away:
                        equipas_a_visitar[home] = liga
                        equipas_a_visitar[away] = liga
                    else:
                        erros_confronto.append(
                            (url, "no_links_found_after_selenium"))
                except Exception as e:
                    erros_confronto.append((url, str(e)))

        # persistir erros se houver
        if erros_confronto:
            os.makedirs("auditoria", exist_ok=True)
            with open(os.path.join("auditoria", f"erros_links_confronto_{date.today()}.csv"), "a", newline="", encoding="utf-8") as f:
                import csv
                writer = csv.writer(f)
                for row in erros_confronto:
                    writer.writerow(row)
            log.warning(
                f"[F2] {len(erros_confronto)} erros ao extrair links de confronto (ver auditoria).")

        t2 = time.time()
        log.info(
            f"[TEMPO] Fase 2 concluída em {(t2 - t1):.2f}s (links extraídos: {len(equipas_a_visitar)})")

        if not equipas_a_visitar:
            print("Não foi possível extrair links de equipas. Rotina concluída.")
            log.warning("Nenhum link de equipa encontrado.")
            return

        # Fase 3: Raspar dados dos times (sequencial por driver)
        print(
            f"\n--- Fase 3: Atualizando histórico de {len(equipas_a_visitar)} equipas ---")
        t1 = time.time()
        jogos_existentes = carregar_jogos_existentes()
        todos_os_jogos_novos = []

        # OBS: raspagem de times envolve 'see more' dinâmico. Mantemos sequencial com o mesmo driver.
        for url, liga_correta in tqdm(equipas_a_visitar.items(), desc="Atualizando Histórico das Equipas"):
            try:
                jogos_da_equipa = dt.raspar_dados_time(
                    driver, url, liga_correta, jogos_existentes, cfg.LIGAS_PERMITIDAS, cfg.LIMITE_JOGOS_POR_TIME)
                todos_os_jogos_novos.extend(jogos_da_equipa)
                # pausa leve para não sobrecarregar
                time.sleep(random.uniform(0.6, 1.2))
            except Exception as e:
                log.error(f"[F3] Erro ao raspar time {url}: {e}")
                with open(os.path.join("auditoria", f"erros_raspagem_times_{date.today()}.csv"), "a", newline="", encoding="utf-8") as f:
                    import csv
                    writer = csv.writer(f)
                    writer.writerow([url, str(e)])

        t2 = time.time()
        log.info(
            f"[TEMPO] Fase 3 concluída em {(t2 - t1):.2f}s (jogos raspados: {len(todos_os_jogos_novos)})")

        # Fase 4: processamento e salvamento
        if todos_os_jogos_novos:
            print(
                f"\n--- Fase 4: Processando e salvando {len(todos_os_jogos_novos)} jogos raspados ---")
            df_novos_jogos = dt.processar_dados_raspados(todos_os_jogos_novos)

            if df_novos_jogos.empty:
                log.warning("Nenhum jogo processado com sucesso.")
            else:
                df_novos_jogos.drop_duplicates(
                    subset=["Data", "Home", "Away"], inplace=True, keep='last')

                jogos_existentes_df = pd.DataFrame(
                    list(jogos_existentes), columns=["Data", "Home", "Away"])
                if not jogos_existentes_df.empty:
                    df_novos_jogos = df_novos_jogos.merge(
                        jogos_existentes_df,
                        on=["Data", "Home", "Away"],
                        how='left',
                        indicator=True
                    ).query('_merge == "left_only"').drop('_merge', axis=1)

                if not df_novos_jogos.empty:
                    salvar_no_banco(df_novos_jogos)
                    print(
                        f"✅ {len(df_novos_jogos)} novos jogos salvos no banco.")
                else:
                    print("Todos os jogos já estavam no banco de dados.")
        else:
            print("\nNenhum resultado novo encontrado para as equipas de amanhã.")

        exportar_para_csv()
        maybe_vacuum_db(NOME_DB)

    except Exception as e:
        log.error(f"Um erro crítico ocorreu na rotina principal: {e}")
        print(f"ERRO CRÍTICO: {e}")

    finally:
        if driver:
            print("\n--- Encerrando o navegador ---")
            try:
                driver.quit()
            except Exception:
                pass

    log.info(
        f"--- Rotina concluída em {(time.time() - start_global):.2f}s ---")
    print("\n--- Rotina diária concluída ---")


if __name__ == "__main__":
    rotina_diaria_noturna()