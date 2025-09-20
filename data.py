import pandas as pd
from bs4 import BeautifulSoup
import ligas_config as cfg
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import csv
from collections import Counter
import os
from datetime import date

# Configura logger específico do módulo
log = logging.getLogger("coletor")
log.setLevel(logging.INFO)
if not log.handlers:
    handler = logging.FileHandler("coletor.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)


# ==========================
# Função para formatar datas
# ==========================
def _formatar_data(texto_data: str):
    """
    Converte datas do site (formato MM-DD-YY ou MM/DD/YY)
    para o formato padronizado YYYY-MM-DD.
    """
    if not texto_data or not isinstance(texto_data, str):
        return None
    try:
        # Forçar interpretação como formato americano (MM-DD-YY)
        data_obj = pd.to_datetime(texto_data, format="%m/%d/%y", errors="coerce")
        if pd.isna(data_obj):
            # fallback: tenta parse genérico
            data_obj = pd.to_datetime(texto_data, errors="coerce")
        if pd.isna(data_obj):
            return None
        return data_obj.strftime("%Y-%m-%d")
    except Exception:
        return None


# ==========================
# Função para converter strings de estatísticas
# ==========================
def _converter_stat_para_int(stat_string):
    """Converte uma string 'A - B' para uma lista [A, B]. Retorna [0, 0] em caso de falha."""
    if not isinstance(stat_string, str) or '-' not in stat_string:
        return [0, 0]
    try:
        partes = [int(p.strip()) for p in stat_string.split('-')]
        return partes if len(partes) == 2 else [0, 0]
    except (ValueError, IndexError):
        return [0, 0]

# ==========================
# Função de Raspagem
# ==========================
def raspar_jogos_de_amanha(driver, ligas_permitidas_set):
    """
    Raspagem da agenda de amanhã na RedScores:
    - Suporte a múltiplos layouts
    - Auditoria de jogos incompletos e times ausentes
    - Deduplicação final com log/CSV
    - Contagem de times únicos
    - Salva auditoria em pastas separadas com arquivos diários
    """
    # Garante que as pastas de auditoria existem
    os.makedirs("jogos_faltando_time", exist_ok=True)
    os.makedirs("jogos_duplicados", exist_ok=True)

    # Nome dos arquivos diários
    data_hoje = date.today().strftime("%Y-%m-%d")
    arquivo_faltando = os.path.join(
        "jogos_faltando_time", f"faltando_time_{data_hoje}.csv")
    arquivo_duplicados = os.path.join(
        "jogos_duplicados", f"duplicados_{data_hoje}.csv")
    arquivo_incompletos = f"jogos_agenda_incompletos_{data_hoje}.csv"

    jogos = []
    total_encontrados = 0
    total_validos = 0
    total_incompletos = 0
    total_filtrados = 0
    times_unicos = set()

    try:
        driver.get("https://redscores.com/pt-br/futebol/amanha")
        log.info("[AGENDA] Aguardando agenda carregar...")

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # 1️⃣ Layout padrão (blocos de liga)
        blocos_liga = soup.select("div[id^='league_']")
        jogos_html = []

        if blocos_liga:
            for bloco in blocos_liga:
                liga_pais = bloco.select_one("span.d-block.d-md-inline")
                liga_nome = bloco.select_one("span.font-bold")
                nome_liga = (
                    f"{liga_pais.get_text(strip=True)} - {liga_nome.get_text(strip=True)}"
                    if liga_pais else liga_nome.get_text(strip=True)
                )

                if nome_liga not in ligas_permitidas_set:
                    total_filtrados += 1
                    continue

                jogos_bloco = bloco.select("tbody[id^='xmatch_']")
                for corpo in jogos_bloco:
                    jogos_html.append((nome_liga, corpo))

        # 2️⃣ Fallback: fixtures__item ou matchLink
        if not jogos_html:
            log.warning(
                "[AGENDA] Nenhum bloco de liga encontrado. Tentando layout alternativo...")
            for jogo in soup.select("div.fixtures__item, a.matchLink"):
                jogos_html.append(("Desconhecida", jogo))

        total_encontrados = len(jogos_html)
        log.info(f"[AGENDA] Total de jogos encontrados: {total_encontrados}")

        for nome_liga, jogo in jogos_html:
            try:
                if jogo.name == "tbody":
                    tds = jogo.select("tr td")
                    hora_texto = tds[1].get_text(strip=True)
                    home = tds[2].select_one("span.team").get_text(strip=True)
                    away = tds[4].select_one("span.team").get_text(strip=True)
                    link_url = "https://redscores.com" + \
                        tds[2].select_one("a")["href"]
                else:
                    hora = jogo.select_one(".fixtures__time")
                    equipes = jogo.select(".fixtures__name")
                    link = jogo.get("href") or (jogo.select_one(
                        "a.fixtures__match") or {}).get("href")
                    hora_texto = hora.get_text(strip=True) if hora else None
                    home, away = (e.get_text(strip=True) for e in equipes[:2]) if len(
                        equipes) >= 2 else (None, None)
                    link_url = "https://redscores.com" + link if link else None

                if not hora_texto or not home or not away or not link_url:
                    total_incompletos += 1
                    log.warning(
                        f"[AGENDA] Jogo incompleto: {nome_liga} | {hora_texto} | {home} x {away} | {link_url}")
                    with open(arquivo_incompletos, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow(
                            [nome_liga, hora_texto, home, away, link_url])
                    continue

                jogos.append({
                    "liga": nome_liga,
                    "hora": hora_texto,
                    "home": home,
                    "away": away,
                    "link_confronto": link_url
                })

                times_unicos.add(home)
                times_unicos.add(away)
                total_validos += 1

            except Exception as e:
                total_incompletos += 1
                log.error(f"[AGENDA] Erro ao processar jogo: {e}")
                with open(arquivo_incompletos, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([nome_liga, "ERRO", str(e)])
                continue

        # Auditoria: verificar se algum jogo válido tem time ausente
        contador_times = Counter()
        for j in jogos:
            contador_times[j["home"]] += 1
            contador_times[j["away"]] += 1


        total_times_contados = sum(contador_times.values())
        log.info(
            f"[AGENDA] Total de ocorrências de times: {total_times_contados} (esperado = {len(jogos) * 2})")

        if total_times_contados != len(jogos) * 2:
            log.warning(
                "[AGENDA] ⚠️ Inconsistência: algum time está faltando ou duplicado!")
            with open(os.path.join("jogos_faltando_time", f"auditoria_times_{data_hoje}.csv"), "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Ocorrencias"])
                for time, qtd in contador_times.most_common():
                    writer.writerow([time, qtd])

        # === Deduplicação final ===
        vistos = set()
        jogos_unicos = []
        duplicatas = []
        for j in jogos:
            chave = (j["liga"], j["hora"], j["home"], j["away"])
            if chave in vistos:
                duplicatas.append(chave)
                with open(arquivo_duplicados, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(
                        [j["liga"], j["hora"], j["home"], j["away"], j["link_confronto"]])
                continue
            vistos.add(chave)
            jogos_unicos.append(j)

        if duplicatas:
            contagem_ligas = Counter([c[0] for c in duplicatas])
            log.warning(
                f"[AGENDA] Duplicatas removidas: {len(duplicatas)} | Ligas afetadas: {dict(contagem_ligas)}")
        else:
            log.info("[AGENDA] Nenhuma duplicata detectada.")

        log.info(
            f"[AGENDA] Concluído. Válidos: {total_validos}, Incompletos: {total_incompletos}, Filtrados: {total_filtrados}")
        log.info(
            f"[AGENDA] Times únicos encontrados: {len(times_unicos)} (esperado ≈ {total_validos * 2})")

        if total_validos == 0:
            with open("snapshot_amanha.html", "w", encoding="utf-8") as f:
                f.write(html)
            log.warning(
                "[AGENDA] Nenhum jogo válido encontrado. HTML salvo para inspeção.")

        return jogos_unicos

    except Exception as e:
        log.error(f"[AGENDA] Falha geral na raspagem: {e}")
        return []

# ==========================
# Obter links de equipes do confronto
# ==========================
def obter_links_equipes_confronto(driver, url_confronto):
    """
    Visita a página de um confronto e extrai os links das páginas das duas equipes.
    - Usa WebDriverWait para aguardar carregamento.
    - Faz log detalhado.
    - Salva casos problemáticos em CSV para auditoria.
    - Tenta fallback para capturar nomes mesmo que links não estejam presentes.
    """
    if not driver:
        log.error(
            f"[DRIVER] Driver não inicializado ao tentar abrir {url_confronto}")
        return None, None

    try:
        driver.get(url_confronto)
        log.info(
            f"[CONFRONTO] Aguardando carregamento da página: {url_confronto}")

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.match-detail__teams"))
        )
        html_content = driver.page_source
        if not html_content or "<body" not in html_content.lower():
            log.error(f"[HTML] Página vazia para {url_confronto}")
            return None, None

        soup = BeautifulSoup(html_content, "html.parser")

        # Seletor mais robusto - pega links mesmo que a classe varie
        links_equipes = soup.select(
            "div.match-detail__teams a, div.match-detail__name a, div.match-detail__team a")

        if len(links_equipes) >= 2:
            link_home = "https://redscores.com" + links_equipes[0]["href"]
            link_away = "https://redscores.com" + links_equipes[1]["href"]
            log.info(
                f"[CONFRONTO] Links encontrados: HOME={link_home}, AWAY={link_away}")
            return link_home, link_away
        else:
            # Fallback para capturar nomes mesmo sem link
            nomes_equipes = [t.get_text(strip=True)
                             for t in soup.select("div.match-detail__name")]
            log.warning(
                f"[CONFRONTO] Não encontrou 2 links para {url_confronto}. Encontrados: {len(links_equipes)}. Nomes detectados: {nomes_equipes}")

            # Salva em CSV para análise futura
            with open("jogos_incompletos.csv", "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [url_confronto, len(links_equipes), ";".join(nomes_equipes)])

            return None, None

    except Exception as e:
        log.error(f"[ERRO] Falha ao processar {url_confronto}: {e}")
        with open("jogos_incompletos.csv", "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([url_confronto, "ERRO", str(e)])
        return None, None

# ==========================
# Obter dados dos times
# ==========================
def raspar_dados_time(driver, time_url, liga_principal, jogos_existentes, ligas_permitidas_set, limite_jogos=cfg.LIMITE_JOGOS_POR_TIME):
    jogos_raspados = []
    if not driver:
        return jogos_raspados
    # Abrir a página do time antes de buscar elementos
    try:
        driver.get(time_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.match-grid__bottom"))
        )
    except Exception:
        # se não conseguiu carregar, segue tentando com o estado atual do driver
        pass

    try:
        while True:
            try:
                jogos_atuais = driver.find_elements(
                    By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")
                if len(jogos_atuais) >= limite_jogos:
                    break

                see_more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "link-see-more")))
                driver.execute_script("arguments[0].click();", see_more_button)
                WebDriverWait(driver, 5).until(lambda d: len(d.find_elements(
                    By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")) > len(jogos_atuais))
            except Exception:
                break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        match_grid = soup.find('div', class_='match-grid__bottom')

        if match_grid:
            for linha in match_grid.select('tbody tr'):
                try:
                    celulas = linha.find_all('td')
                    if len(celulas) > 10:
                        liga_img = celulas[1].find('img')
                        liga_local = liga_img['alt'].strip(
                        ) if liga_img else ''
                        if not liga_local:
                            continue

                        liga_final_para_salvar = None

                        # 1. Verifica se a liga local pertence à liga principal da equipa
                        if liga_local.lower() in liga_principal.lower():
                            liga_final_para_salvar = liga_principal
                        else:
                            # 2. Se não, verifica se a liga local (ex: Copa Libertadores) está na lista de permitidas
                            for liga_permitida in ligas_permitidas_set:
                                if liga_local.lower() in liga_permitida.lower():
                                    liga_final_para_salvar = liga_permitida
                                    break

                        # 3. Se a liga do jogo não for encontrada nas nossas regras, pula para a próxima linha
                        if not liga_final_para_salvar:
                            continue

                        data = celulas[0].text.strip()
                        time_casa = celulas[2].text.strip()
                        time_fora = celulas[4].text.strip()

                        data_padronizada = _formatar_data(data)
                        home_normalizado = " ".join(time_casa.split()).title()
                        away_normalizado = " ".join(time_fora.split()).title()
                        identificador_jogo_atual = (
                            data_padronizada, home_normalizado, away_normalizado)

                        if identificador_jogo_atual in jogos_existentes:
                            break

                        placar_texto = celulas[3].text.strip()
                        placar_ht = celulas[5].text.strip()
                        chutes = celulas[6].text.strip()
                        chutes_gol = celulas[7].text.strip()
                        ataques = celulas[8].text.strip()
                        escanteios = celulas[9].text.strip()
                        odd_h = celulas[11].text.strip()
                        odd_d = celulas[12].text.strip()
                        odd_a = celulas[13].text.strip()

                        jogos_raspados.append({
                            "Liga": liga_final_para_salvar, "Data": data, "Home": time_casa, "Away": time_fora,
                            "Placar_FT": placar_texto, "Placar_HT": placar_ht, "Chutes": chutes,
                            "Chutes_Gol": chutes_gol, "Ataques": ataques, "Escanteios": escanteios,
                            "Odd_H_str": odd_h, "Odd_D_str": odd_d, "Odd_A_str": odd_a
                        })
                except Exception:
                    continue
    except Exception as e:
        print(f"Ocorreu um erro geral com o Selenium em {time_url}: {e}")
    return jogos_raspados

# ==========================
# Processamento dos dados
# ==========================
def processar_dados_raspados(lista_de_jogos):
    jogos_processados = []
    for jogo in lista_de_jogos:
        try:
            data_padronizada = _formatar_data(jogo['Data'])
            if data_padronizada is None:
                continue

            liga = " ".join(jogo['Liga'].split()).title()
            placar_ft = _converter_stat_para_int(jogo['Placar_FT'])
            placar_ht = _converter_stat_para_int(jogo['Placar_HT'])
            chutes = _converter_stat_para_int(jogo['Chutes'])
            chutes_gol = _converter_stat_para_int(jogo['Chutes_Gol'])
            ataques = _converter_stat_para_int(jogo['Ataques'])
            escanteios = _converter_stat_para_int(jogo['Escanteios'])

            odd_h = float(
                jogo['Odd_H_str']) if jogo['Odd_H_str'] and jogo['Odd_H_str'] != '-' else 0.0
            odd_d = float(
                jogo['Odd_D_str']) if jogo['Odd_D_str'] and jogo['Odd_D_str'] != '-' else 0.0
            odd_a = float(
                jogo['Odd_A_str']) if jogo['Odd_A_str'] and jogo['Odd_A_str'] != '-' else 0.0

            home = " ".join(jogo['Home'].split())
            away = " ".join(jogo['Away'].split())

            jogos_processados.append({
                "Liga": liga, "Data": data_padronizada, "Home": home, "Away": away,
                "H_Gols_FT": placar_ft[0], "A_Gols_FT": placar_ft[1],
                "H_Gols_HT": placar_ht[0], "A_Gols_HT": placar_ht[1],
                "H_Chute": chutes[0], "A_Chute": chutes[1],
                "H_Chute_Gol": chutes_gol[0], "A_Chute_Gol": chutes_gol[1],
                "H_Ataques": ataques[0], "A_Ataques": ataques[1],
                "H_Escanteios": escanteios[0], "A_Escanteios": escanteios[1],
                "Odd_H": odd_h, "Odd_D": odd_d, "Odd_A": odd_a
            })
        except Exception as e:
            print(f"[DIAGNÓSTICO] ERRO ao processar o jogo acima: {e}")
            continue
    return pd.DataFrame(jogos_processados)