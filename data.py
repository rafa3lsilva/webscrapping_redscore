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
from datetime import date, timedelta, datetime

# ==========================
# Logger
# ==========================
# Usa logger filho do coletor — o handler é configurado em coletor.py (root logger)
log = logging.getLogger("coletor.data")
dia = date.today() + timedelta(days=1)

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
        #driver.get("https://redscores.com/pt-br/")
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
                    "data": dia,
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

        duplicatas = []
        
        # Manter o log de itens descartados
        with open(arquivo_duplicados, "w", newline="", encoding="utf-8") as f:
            # Escreve o cabeçalho no arquivo de duplicados
            csv.writer(f).writerow(["liga", "hora", "home", "away", "link_confronto", "motivo"])

        for jogo_atual in jogos:
            chave = (jogo_atual.get("liga"), jogo_atual.get("hora"), jogo_atual.get("home"), jogo_atual.get("away"))

            if chave not in jogos_unicos_dict:
                # Se é a primeira vez que vemos este jogo, simplesmente o adicionamos.
                jogos_unicos_dict[chave] = jogo_atual
            else:
                # Já existe uma versão deste jogo, vamos comparar.
                jogo_existente = jogos_unicos_dict[chave]
                # Condição para o jogo existente ter odds válidas.
                existente_tem_odds = jogo_existente.get('Odd_H') not in [None, 0, 1.0, '']
                # Condição para o jogo atual (o novo) ter odds válidas.
                atual_tem_odds = jogo_atual.get('Odd_H') not in [None, 0, 1.0, '']

                if not existente_tem_odds and atual_tem_odds:
                    jogos_unicos_dict[chave] = jogo_atual
                    
                    # Logamos o jogo antigo como "substituído por versão com odds"
                    with open(arquivo_duplicados, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow([*chave, jogo_existente.get("link_confronto", "N/A"), "Substituído por versão com odds"])
                else:
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
                
                chave = (data_padronizada, home_norm, away_norm)
                if chave in jogos_existentes and jogos_existentes[chave] is True:
                    continue  # Já existe e está completo, pula.

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

            # Converter odds com tratamento seguro
            def _parse_odd(valor_str):
                if valor_str in [None, "-", "", "0", "0.0"]:
                    return None
                try:
                    v = float(valor_str)
                    return v if v > 0 else None
                except (ValueError, TypeError):
                    return None

            odd_h = _parse_odd(jogo.get('Odd_H_str'))
            odd_d = _parse_odd(jogo.get('Odd_D_str'))
            odd_a = _parse_odd(jogo.get('Odd_A_str'))

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
                "Odd_H": odd_h,
                "Odd_D": odd_d,
                "Odd_A": odd_a,
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

    # Validação pós-processamento
    df = pd.DataFrame(jogos_processados)
    if not df.empty:
        sem_odds = df[df['Odd_H'].isna() | df['Odd_D'].isna() | df['Odd_A'].isna()]
        if len(sem_odds) > 0:
            log.warning(f"[VALIDAÇÃO] {len(sem_odds)} jogos sem odds válidas (serão salvos com NULL).")

    return df


# ==========================
# Raspar detalhes do confronto
def raspar_detalhes_confronto(driver, url_confronto, rodada_nome=None):
    """
    Raspa as estatísticas de um jogo específico acessando sua página detalhada.
    Retorna um dicionário com os dados do jogo.
    """
    try:
        driver.get(url_confronto)
        wait = WebDriverWait(driver, 10)
        
        # Espera carregar o cabeçalho
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".match-detail__header")))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # 1. Verificar Status da Partida (Ignorar se não estiver finalizada)
        status_elem = soup.select_one("p.match-detail__status")
        status_text = status_elem.get_text(strip=True).upper() if status_elem else ""
        
        # O site usa "MATCH REPORT" para jogos finalizados
        if "MATCH REPORT" not in status_text and "ENCERRADO" not in status_text and "FINALIZADO" not in status_text:
            log.warning(f"Jogo ignorado (Não finalizado): {url_confronto} - Status: {status_text}")
            return None
        
        # 2. Cabeçalho e Data
        header_p = soup.select_one(".match-detail__header p")
        data_bruta = header_p.get_text(strip=True) if header_p else ""
        
        rodada_extraida = "N/A"
        data_hora_texto = ""
        if "-" in data_bruta:
            partes = [p.strip() for p in data_bruta.split("-")]
            rodada_extraida = partes[1] if len(partes) > 1 else "N/A"
            data_hora_texto = partes[0]
        else:
            data_hora_texto = data_bruta

        # Normalizar data para YYYY-MM-DD (RedScore usa MM/DD/YY)
        data_iso = None
        try:
            if data_hora_texto:
                partes_espaco = data_hora_texto.split()
                if partes_espaco:
                    partes_data = partes_espaco[0].split('/')
                    if len(partes_data) == 3:
                        m, d, y = partes_data
                        ano_completo = f"20{y}" if len(y) == 2 else y
                        data_iso = f"{ano_completo}-{m.zfill(2)}-{d.zfill(2)}"
        except Exception as e:
            log.warning(f"Falha ao processar data '{data_hora_texto}': {e}")
        
        if not data_iso:
            data_iso = date.today().strftime("%Y-%m-%d")

        # 3. Times (Garantir ordem Home/Away)
        home_elem = soup.select_one(".match-detail__team--home .match-detail__name a")
        away_elem = soup.select_one(".match-detail__team--away .match-detail__name a")
        
        if not home_elem or not away_elem:
            times = soup.select(".match-detail__name a")
            home = times[0].get_text(strip=True) if len(times) > 0 else "Desconhecido"
            away = times[1].get_text(strip=True) if len(times) > 1 else "Desconhecido"
        else:
            home = home_elem.get_text(strip=True)
            away = away_elem.get_text(strip=True)
        
        # 4. Placar Final (FT)
        score_elem = soup.select_one(".match-detail__score")
        placar_ft_text = score_elem.get_text(strip=True) if score_elem else "0-0"
        try:
            if "-" in placar_ft_text:
                partes_ft = placar_ft_text.split("-")
                h_gols_ft = int(partes_ft[0].strip())
                a_gols_ft = int(partes_ft[1].strip())
            else:
                h_gols_ft, a_gols_ft = 0, 0
        except:
            h_gols_ft, a_gols_ft = 0, 0

        # 5. HT Score
        placar_ht = "0-0"
        ht_elem = soup.select_one("td.half-time span, .match-summary__score--ht")
        if ht_elem:
            placar_ht = ht_elem.get_text(strip=True).upper().replace("HT", "").replace("(", "").replace(")", "").replace(" ", "")
        else:
            resumo_texto = soup.select_one(".match-summary")
            if resumo_texto and "(" in resumo_texto.get_text():
                import re
                match_ht = re.search(r"\((\d+-\d+)\)", resumo_texto.get_text())
                if match_ht:
                    placar_ht = match_ht.group(1)

        # 6. Estatísticas
        final_stats = {
            "Total de chutes": "0-0",
            "Chutes a gol": "0-0",
            "Ataques": "0-0",
            "Escanteios": "0-0"
        }

        elementos_stats = soup.select(".stats-ranking-item, div.col-4.text-center")
        for elem in elementos_stats:
            label_elem = elem.select_one(".progress-title") or elem.find(string=True, recursive=False)
            if not label_elem: continue
            
            label = label_elem.get_text(strip=True) if hasattr(label_elem, "get_text") else str(label_elem).strip()
            val_h_elem = elem.select_one(".progress-bar__value_left, .progress-value-left")
            val_a_elem = elem.select_one(".progress-bar__value_right, .progress-value-right")
            
            if val_h_elem and val_a_elem:
                val_h = val_h_elem.get_text(strip=True).replace("%", "")
                val_a = val_a_elem.get_text(strip=True).replace("%", "")
                valor = f"{val_h}-{val_a}"
                
                l_low = label.lower()
                if "chute" in l_low or "shot" in l_low:
                    if "gol" in l_low or "on goal" in l_low: final_stats["Chutes a gol"] = valor
                    else: final_stats["Total de chutes"] = valor
                elif "ataque" in l_low or "attack" in l_low: final_stats["Ataques"] = valor
                elif "escanteio" in l_low or "corner" in l_low: final_stats["Escanteios"] = valor

        # Normalizar rodada
        if not rodada_nome:
            num_rodada = rodada_extraida.split('.')[0] if '.' in rodada_extraida else rodada_extraida
            rodada_nome = f"Rodada {num_rodada}" if num_rodada.isdigit() else num_rodada
        
        # 5. Odds (Aba Odds)
        odd_h, odd_d, odd_a = None, None, None
        try:
            tabela_odds = soup.select_one("table.table-data__table--odds")
            if not tabela_odds:
                # Tenta clicar na aba Odds se não estiver visível
                try:
                    btn_odds = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Odds')]")))
                    driver.execute_script("arguments[0].click();", btn_odds)
                    time.sleep(1)
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    tabela_odds = soup.select_one("table.table-data__table--odds")
                except:
                    pass
            
            if tabela_odds:
                linhas = tabela_odds.select("tbody tr")
                for r in linhas:
                    header_linha = r.select_one("th")
                    if header_linha and ("antes do jogo" in header_linha.get_text().lower() or "closing" in header_linha.get_text().lower()):
                        celulas = r.select("td")
                        if len(celulas) >= 3:
                            def _limpar_odd(txt):
                                return float(txt.replace("↑", "").replace("↓", "").strip())
                            odd_h = _limpar_odd(celulas[0].get_text())
                            odd_d = _limpar_odd(celulas[1].get_text())
                            odd_a = _limpar_odd(celulas[2].get_text())
                            break
        except Exception as e:
            log.debug(f"Odds não encontradas para {home} vs {away}: {e}")

        return {
            "Data": data_iso,
            "Home": home,
            "Away": away,
            "Rodada": rodada_nome,
            "Placar_FT": f"{h_gols_ft}-{a_gols_ft}",
            "Placar_HT": placar_ht,
            "Chutes": final_stats["Total de chutes"],
            "Chutes_Gol": final_stats["Chutes a gol"],
            "Ataques": final_stats["Ataques"],
            "Escanteios": final_stats["Escanteios"],
            "Odd_H": odd_h,
            "Odd_D": odd_d,
            "Odd_A": odd_a
        }

    except Exception as e:
        log.error(f"Erro ao raspar detalhes do confronto {url_confronto}: {e}")
        return None
