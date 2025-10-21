import pandas as pd
from bs4 import BeautifulSoup
import ligas_config as cfg
import time
import logging
import csv
from collections import Counter
import os
from datetime import date
import unicodedata
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==========================
# Logger
# ==========================
log = logging.getLogger("coletor")
log.setLevel(logging.INFO)
if not log.handlers:
    handler = logging.FileHandler("coletor.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)

# ==========================
# Utilitários
# ==========================
def _normalizar(texto: str) -> str:
    """Remove acentos, transforma em lowercase e remove espaços extras."""
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize("NFKD", texto).encode(
        "ASCII", "ignore").decode("utf-8")
    return " ".join(texto.lower().split())


def _formatar_data(texto_data: str):
    if not texto_data or not isinstance(texto_data, str):
        return None
    try:
        data_obj = pd.to_datetime(texto_data, errors="coerce", dayfirst=False)
        if pd.isna(data_obj):
            return None
        return data_obj.strftime("%Y-%m-%d")
    except Exception:
        return None


def _converter_stat_para_int(stat_string):
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
    os.makedirs("jogos_faltando_time", exist_ok=True)
    os.makedirs("jogos_duplicados", exist_ok=True)
    os.makedirs("ligas_ignoradas", exist_ok=True)

    data_hoje = date.today().strftime("%Y-%m-%d")
    arquivo_faltando = os.path.join(
        "jogos_faltando_time", f"faltando_time_{data_hoje}.csv")
    arquivo_duplicados = os.path.join(
        "jogos_duplicados", f"duplicados_{data_hoje}.csv")
    arquivo_incompletos = f"jogos_agenda_incompletos_{data_hoje}.csv"
    arquivo_ignoradas = os.path.join(
        "ligas_ignoradas", f"ligas_ignoradas_{data_hoje}.csv")

    jogos = []
    total_validos, total_incompletos, total_filtrados = 0, 0, 0
    times_unicos = set()

    try:
        driver.get("https://redscores.com/pt-br/futebol/amanha")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        blocos_liga = soup.select("div[id^='league_']")
        jogos_html = []

        if blocos_liga:
            for bloco in blocos_liga:
                liga_pais = bloco.select_one("span.d-block.d-md-inline")
                liga_nome = bloco.select_one("span.font-bold")
                nome_liga = f"{liga_pais.get_text(strip=True)} - {liga_nome.get_text(strip=True)}" if liga_pais else liga_nome.get_text(
                    strip=True)

                if _normalizar(nome_liga) not in {_normalizar(l) for l in ligas_permitidas_set}:
                    total_filtrados += 1
                    with open(arquivo_ignoradas, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow([nome_liga])
                    continue

                jogos_bloco = bloco.select("tbody[id^='xmatch_']")
                for corpo in jogos_bloco:
                    jogos_html.append((nome_liga, corpo))

        if not jogos_html:
            log.warning(
                "[AGENDA] Nenhum bloco de liga encontrado. Salvando snapshot...")
            with open("snapshot_amanha.html", "w", encoding="utf-8") as f:
                f.write(html)

        for nome_liga, jogo in jogos_html:
            try:
                tds = jogo.select("tr td")
                hora_texto = tds[1].get_text(strip=True)
                home = tds[2].select_one("span.team").get_text(strip=True)
                away = tds[4].select_one("span.team").get_text(strip=True)
                link_url = "https://redscores.com" + \
                    tds[2].select_one("a")["href"]

                if not all([hora_texto, home, away, link_url]):
                    total_incompletos += 1
                    with open(arquivo_incompletos, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow(
                            [nome_liga, hora_texto, home, away, link_url])
                    continue
                
                # Inicializa as odds como None
                odd_h, odd_d, odd_a = None, None, None
                try:
                    # Tenta selecionar o texto dentro das colunas 15, 16 e 17
                    # O índice em Python é n-1, então usamos 14, 15, 16
                    odd_h_text = tds[14].get_text(strip=True)
                    odd_d_text = tds[15].get_text(strip=True)
                    odd_a_text = tds[16].get_text(strip=True)
                    
                    # Converte para float se o texto não estiver vazio
                    if odd_h_text: odd_h = float(odd_h_text)
                    if odd_d_text: odd_d = float(odd_d_text)
                    if odd_a_text: odd_a = float(odd_a_text)
                    log.info(f"[ODDS] Odds para {home} vs {away}: {odd_h}, {odd_d}, {odd_a}")

                except (IndexError, ValueError) as e:
                    # IndexError: acontece se o jogo não tiver as 17 colunas (sem odds)
                    # ValueError: acontece se o texto não puder ser convertido para float
                    log.warning(f"[ODDS] Odds não encontradas para {home} vs {away}. Motivo: {e}")

                jogos.append({
                    "liga": nome_liga,
                    "hora": hora_texto,
                    "home": home,
                    "away": away,
                    "Odd_H": odd_h,
                    "Odd_D": odd_d,
                    "Odd_A": odd_a,
                    "link_confronto": link_url
                })
                times_unicos.update([home, away])
                total_validos += 1
            except Exception as e:
                total_incompletos += 1
                log.error(f"[AGENDA] Erro ao processar jogo: {e}")
                with open(arquivo_incompletos, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([nome_liga, "ERRO", str(e)])

        # Auditoria de times
        contador_times = Counter()
        for j in jogos:
            contador_times[j["home"]] += 1
            contador_times[j["away"]] += 1

        total_times_contados = sum(contador_times.values())
        if total_times_contados != len(jogos) * 2:
            log.warning(
                f"[AGENDA] ⚠️ Diferença detectada: {total_times_contados} vs esperado {len(jogos) * 2}")
            with open(os.path.join("jogos_faltando_time", f"auditoria_times_{data_hoje}.csv"), "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Ocorrencias"])
                for time, qtd in contador_times.most_common():
                    writer.writerow([time, qtd])

        jogos_unicos_dict = {}
        
        # Manter o log de itens descartados
        with open(arquivo_duplicados, "w", newline="", encoding="utf-8") as f:
            # Escreve o cabeçalho no arquivo de duplicados
            csv.writer(f).writerow(["liga", "hora", "home", "away", "link_confronto", "motivo"])

        for jogo_atual in jogos:
            chave = (jogo_atual["liga"], jogo_atual["hora"], jogo_atual["home"], jogo_atual["away"])

            if chave not in jogos_unicos_dict:
                # Se é a primeira vez que vemos este jogo, simplesmente o adicionamos.
                jogos_unicos_dict[chave] = jogo_atual
            else:
                # Já existe uma versão deste jogo, vamos comparar.
                jogo_existente = jogos_unicos_dict[chave]

                # --- Verificação de Odds (ADAPTE ESTA LÓGICA SE NECESSÁRIO) ---
                # Condição para o jogo existente ter odds válidas.
                existente_tem_odds = 'odd_home' in jogo_existente and jogo_existente['odd_home'] not in [None, 0, 1.0]
                # Condição para o jogo atual (o novo) ter odds válidas.
                atual_tem_odds = 'odd_home' in jogo_atual and jogo_atual['odd_home'] not in [None, 0, 1.0]
                # --- Fim da Verificação ---

                if not existente_tem_odds and atual_tem_odds:
                    # O jogo existente NÃO tem odds, mas o novo TEM.
                    # Substituímos o existente pelo novo, que é mais completo.
                    jogos_unicos_dict[chave] = jogo_atual
                    
                    # Logamos o jogo antigo como "substituído por versão com odds"
                    with open(arquivo_duplicados, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow([*chave, jogo_existente.get("link_confronto", "N/A"), "Substituído por versão com odds"])
                else:
                    # Em todos os outros casos (ambos têm odds, ou só o existente tem, ou nenhum tem),
                    # mantemos a primeira versão que encontrámos e descartamos a nova.
                    with open(arquivo_duplicados, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow([*chave, jogo_atual.get("link_confronto", "N/A"), "Duplicado sem prioridade"])

        # No final, a lista de jogos únicos e de melhor qualidade são os valores do nosso dicionário.
        jogos_unicos = list(jogos_unicos_dict.values())

        log.info(
            f"[AGENDA] Válidos={total_validos}, Incompletos={total_incompletos}, Filtrados={total_filtrados}, Duplicados={len(duplicatas)}")
        return jogos_unicos

    except Exception as e:
        log.error(f"[AGENDA] Falha geral: {e}")
        return []

# ==========================
# Obter links de equipes com retry
# ==========================
def obter_links_equipes_confronto(driver, url_confronto, tentativas=2):
    for tentativa in range(tentativas):
        try:
            driver.get(url_confronto)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.match-detail__teams"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")
            links_equipes = soup.select(
                "div.match-detail__teams a, div.match-detail__name a")
            if len(links_equipes) >= 2:
                return "https://redscores.com" + links_equipes[0]["href"], "https://redscores.com" + links_equipes[1]["href"]
        except Exception as e:
            log.warning(
                f"[CONFRONTO] Tentativa {tentativa+1} falhou para {url_confronto}: {e}")
            time.sleep(2)
    log.error(
        f"[CONFRONTO] Falhou após {tentativas} tentativas: {url_confronto}")
    with open("jogos_incompletos.csv", "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([url_confronto, "LINKS_NAO_ENCONTRADOS"])
    return None, None

# ==========================
# Raspar dados do time
# ==========================
def raspar_dados_time(driver, time_url, liga_principal, jogos_existentes, ligas_permitidas_set, limite_jogos=cfg.LIMITE_JOGOS_POR_TIME):
    jogos_raspados = []
    try:
        driver.get(time_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div.match-grid__bottom")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for linha in soup.select("div.match-grid__bottom tbody tr"):
            try:
                celulas = linha.find_all('td')
                if len(celulas) <= 10:
                    continue
                liga_img = celulas[1].find('img')
                liga_local = liga_img['alt'].strip() if liga_img else ''
                if not liga_local:
                    continue

                liga_final = None
                if liga_local.lower() in liga_principal.lower():
                    liga_final = liga_principal
                else:
                    for liga_permitida in ligas_permitidas_set:
                        if liga_local.lower() in liga_permitida.lower():
                            liga_final = liga_permitida
                            break
                if not liga_final:
                    continue

                data = celulas[0].text.strip()
                time_casa = celulas[2].text.strip()
                time_fora = celulas[4].text.strip()
                data_padronizada = _formatar_data(data)
                home_norm, away_norm = " ".join(
                    time_casa.split()).title(), " ".join(time_fora.split()).title()
                if (data_padronizada, home_norm, away_norm) in jogos_existentes:
                    continue  # <-- não interrompe raspagem de outros jogos

                jogos_raspados.append({
                    "Liga": liga_final, "Data": data, "Home": time_casa, "Away": time_fora,
                    "Placar_FT": celulas[3].text.strip(),
                    "Placar_HT": celulas[5].text.strip(),
                    "Chutes": celulas[6].text.strip(),
                    "Chutes_Gol": celulas[7].text.strip(),
                    "Ataques": celulas[8].text.strip(),
                    "Escanteios": celulas[9].text.strip(),
                    "Odd_H_str": celulas[11].text.strip(),
                    "Odd_D_str": celulas[12].text.strip(),
                    "Odd_A_str": celulas[13].text.strip()
                })
            except Exception as e:
                log.error(f"[TIME] Erro ao processar linha em {time_url}: {e}")
                with open("erros_raspagem_times.csv", "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([time_url, str(e)])
    except Exception as e:
        log.error(f"[TIME] Falha geral ao abrir {time_url}: {e}")
    return jogos_raspados

# ==========================
# Processamento dos dados
# ==========================
def processar_dados_raspados(lista_de_jogos):
    jogos_processados, descartados = [], []
    for jogo in lista_de_jogos:
        try:
            data_padronizada = _formatar_data(jogo['Data'])
            if not data_padronizada:
                descartados.append(jogo)
                continue
            jogos_processados.append({
                "Liga": " ".join(jogo['Liga'].split()).title(),
                "Data": data_padronizada,
                "Home": " ".join(jogo['Home'].split()),
                "Away": " ".join(jogo['Away'].split()),
                "H_Gols_FT": _converter_stat_para_int(jogo['Placar_FT'])[0],
                "A_Gols_FT": _converter_stat_para_int(jogo['Placar_FT'])[1],
                "H_Gols_HT": _converter_stat_para_int(jogo['Placar_HT'])[0],
                "A_Gols_HT": _converter_stat_para_int(jogo['Placar_HT'])[1],
                "H_Chute": _converter_stat_para_int(jogo['Chutes'])[0],
                "A_Chute": _converter_stat_para_int(jogo['Chutes'])[1],
                "H_Chute_Gol": _converter_stat_para_int(jogo['Chutes_Gol'])[0],
                "A_Chute_Gol": _converter_stat_para_int(jogo['Chutes_Gol'])[1],
                "H_Ataques": _converter_stat_para_int(jogo['Ataques'])[0],
                "A_Ataques": _converter_stat_para_int(jogo['Ataques'])[1],
                "H_Escanteios": _converter_stat_para_int(jogo['Escanteios'])[0],
                "A_Escanteios": _converter_stat_para_int(jogo['Escanteios'])[1],
                "Odd_H": float(jogo['Odd_H_str']) if jogo['Odd_H_str'] not in [None, "-"] else 0.0,
                "Odd_D": float(jogo['Odd_D_str']) if jogo['Odd_D_str'] not in [None, "-"] else 0.0,
                "Odd_A": float(jogo['Odd_A_str']) if jogo['Odd_A_str'] not in [None, "-"] else 0.0,
            })
        except Exception as e:
            descartados.append(jogo)
            log.error(f"[PROCESSAMENTO] Falha ao processar jogo: {e}")
    if descartados:
        with open(f"jogos_processamento_falhos_{date.today()}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=descartados[0].keys())
            writer.writeheader()
            writer.writerows(descartados)
        log.warning(
            f"[PROCESSAMENTO] {len(descartados)} jogos descartados. CSV salvo.")
    return pd.DataFrame(jogos_processados)