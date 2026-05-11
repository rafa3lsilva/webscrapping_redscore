import time
import logging
import sqlite3
import pandas as pd
import requests
import re
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
import random
from ligas_config import LIGAS_XG

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("coletor_flashscore.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DB_NAME = "flashscore_dados.db"

def limpar_valor(val):
    if val is None or val == "": return None
    if isinstance(val, (int, float)): return val
    limpo = "".join(c for c in str(val) if c.isdigit() or c == "." or c == ",")
    limpo = limpo.replace(",", ".")
    try:
        return float(limpo) if "." in limpo else int(limpo)
    except:
        return None

def inicializar_banco():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jogos_flashscore (
            Match_ID TEXT PRIMARY KEY, Data TEXT, Hora TEXT, Pais TEXT, Liga TEXT, Temporada TEXT, Rodada TEXT, Home TEXT, Away TEXT,
            Gols_Home_FT INTEGER, Gols_Away_FT INTEGER, Gols_Home_HT INTEGER, Gols_Away_HT INTEGER,
            Odd_H_Open REAL, Odd_D_Open REAL, Odd_A_Open REAL, Odd_H_Close REAL, Odd_D_Close REAL, Odd_A_Close REAL,
            Over_25_Open REAL, Over_25_Close REAL, Under_25_Open REAL, Under_25_Close REAL,
            BTTS_Sim_Open REAL, BTTS_Sim_Close REAL, BTTS_Nao_Open REAL, BTTS_Nao_Close REAL,
            AH_H_Open REAL, AH_H_Close REAL, AH_A_Open REAL, AH_A_Close REAL,
            DC_1X_Open REAL, DC_12_Open REAL, DC_X2_Open REAL, DC_1X_Close REAL, DC_12_Close REAL, DC_X2_Close REAL,
            Over_05HT_Open REAL, Over_05HT_Close REAL, Over_15HT_Open REAL, Over_15HT_Close REAL,
            FT_xG_Home REAL, FT_xG_Away REAL, FT_xGOT_Home REAL, FT_xGOT_Away REAL, FT_Posse_Home REAL, FT_Posse_Away REAL, 
            FT_Chutes_Home INTEGER, FT_Chutes_Away INTEGER, FT_Chutes_Alvo_Home INTEGER, FT_Chutes_Alvo_Away INTEGER,
            FT_Chances_Claras_Home INTEGER, FT_Chances_Claras_Away INTEGER, FT_Toques_Area_Home INTEGER, FT_Toques_Area_Away INTEGER,
            FT_Escanteios_Home INTEGER, FT_Escanteios_Away INTEGER, FT_Defesas_Home INTEGER, FT_Defesas_Away INTEGER,
            HT_xG_Home REAL, HT_xG_Away REAL, HT_Posse_Home REAL, HT_Posse_Away REAL,
            HT_Chutes_Home INTEGER, HT_Chutes_Away INTEGER, HT_Chutes_Alvo_Home INTEGER, HT_Chutes_Alvo_Away INTEGER,
            HT_Escanteios_Home INTEGER, HT_Escanteios_Away INTEGER
        )
        """)
        conn.commit()

def extrair_odds_ninja(match_id, session):
    """Extrai odds usando a API GraphQL (NINJA) - 1X2, OU, BTTS, AH, DC."""
    res = {}
    base_params = {"eventId": match_id, "projectId": "401", "geoIpCode": "BR", "geoIpSubdivisionCode": "BR"}
    headers = {"user-agent": "Mozilla/5.0", "referer": f"https://www.flashscore.com.br/jogo/{match_id}/", "Origin": "https://www.flashscore.com.br"}
    url = "https://global.ds.lsapp.eu/odds/pq_graphql"

    # Tentamos com alguns hashes conhecidos
    hashes = ["oce", "pobtm", "ope2"]
    
    for h in hashes:
        try:
            params = base_params.copy()
            params.update({"_hash": h})
            r = session.get(url, params=params, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json().get("data", {}).get("findOddsByEventId", {}).get("odds", [])
                if not data: continue
                
                for market in data:
                    bet_type = market.get("bettingType")
                    odds_list = market.get("odds", [])
                    
                    if bet_type == "HOME_DRAW_AWAY" and len(odds_list) >= 3:
                        res["Odd_H_Open"] = res.get("Odd_H_Open") or limpar_valor(odds_list[0].get("opening"))
                        res["Odd_H_Close"] = res.get("Odd_H_Close") or limpar_valor(odds_list[0].get("value"))
                        res["Odd_D_Open"] = res.get("Odd_D_Open") or limpar_valor(odds_list[1].get("opening"))
                        res["Odd_D_Close"] = res.get("Odd_D_Close") or limpar_valor(odds_list[1].get("value"))
                        res["Odd_A_Open"] = res.get("Odd_A_Open") or limpar_valor(odds_list[2].get("opening"))
                        res["Odd_A_Close"] = res.get("Odd_A_Close") or limpar_valor(odds_list[2].get("value"))
                    
                    elif bet_type == "DOUBLE_CHANCE" and len(odds_list) >= 3:
                        res["DC_1X_Open"] = res.get("DC_1X_Open") or limpar_valor(odds_list[0].get("opening"))
                        res["DC_1X_Close"] = res.get("DC_1X_Close") or limpar_valor(odds_list[0].get("value"))
                        res["DC_12_Open"] = res.get("DC_12_Open") or limpar_valor(odds_list[1].get("opening"))
                        res["DC_12_Close"] = res.get("DC_12_Close") or limpar_valor(odds_list[1].get("value"))
                        res["DC_X2_Open"] = res.get("DC_X2_Open") or limpar_valor(odds_list[2].get("opening"))
                        res["DC_X2_Close"] = res.get("DC_X2_Close") or limpar_valor(odds_list[2].get("value"))

                    elif bet_type == "BOTH_TEAMS_TO_SCORE" and len(odds_list) >= 2:
                        res["BTTS_Sim_Open"] = res.get("BTTS_Sim_Open") or limpar_valor(odds_list[0].get("opening"))
                        res["BTTS_Sim_Close"] = res.get("BTTS_Sim_Close") or limpar_valor(odds_list[0].get("value"))
                        res["BTTS_Nao_Open"] = res.get("BTTS_Nao_Open") or limpar_valor(odds_list[1].get("opening"))
                        res["BTTS_Nao_Close"] = res.get("BTTS_Nao_Close") or limpar_valor(odds_list[1].get("value"))

                    elif bet_type == "OVER_UNDER" and len(odds_list) > 0:
                        for ou_row in odds_list:
                            name = str(ou_row.get("name"))
                            if name == "2.5":
                                res["Over_25_Open"] = res.get("Over_25_Open") or limpar_valor(ou_row.get("over", {}).get("opening"))
                                res["Over_25_Close"] = res.get("Over_25_Close") or limpar_valor(ou_row.get("over", {}).get("value"))
                                res["Under_25_Open"] = res.get("Under_25_Open") or limpar_valor(ou_row.get("under", {}).get("opening"))
                                res["Under_25_Close"] = res.get("Under_25_Close") or limpar_valor(ou_row.get("under", {}).get("value"))
                            elif name == "0.5" and "HT" in market.get("periodType", ""):
                                res["Over_05HT_Open"] = res.get("Over_05HT_Open") or limpar_valor(ou_row.get("over", {}).get("opening"))
                                res["Over_05HT_Close"] = res.get("Over_05HT_Close") or limpar_valor(ou_row.get("over", {}).get("value"))
                            elif name == "1.5" and "HT" in market.get("periodType", ""):
                                res["Over_15HT_Open"] = res.get("Over_15HT_Open") or limpar_valor(ou_row.get("over", {}).get("opening"))
                                res["Over_15HT_Close"] = res.get("Over_15HT_Close") or limpar_valor(ou_row.get("over", {}).get("value"))

                    elif bet_type == "ASIAN_HANDICAP" and len(odds_list) > 0:
                        for ah_row in odds_list:
                            if str(ah_row.get("name")) in ["0", "0.0"]:
                                res["AH_H_Open"] = res.get("AH_H_Open") or limpar_valor(ah_row.get("home", {}).get("opening"))
                                res["AH_H_Close"] = res.get("AH_H_Close") or limpar_valor(ah_row.get("home", {}).get("value"))
                                res["AH_A_Open"] = res.get("AH_A_Open") or limpar_valor(ah_row.get("away", {}).get("opening"))
                                res["AH_A_Close"] = res.get("AH_A_Close") or limpar_valor(ah_row.get("away", {}).get("value"))
        except: continue
    return res

def coletar_detalhes_ninja(match_id, session):
    """Coleta Stats Avançadas e Placar via GraphQL."""
    detalhes = {}
    url = "https://global.ds.lsapp.eu/pq_graphql"
    params = {"_hash": "dsos2", "eventId": match_id, "projectId": "401"}
    headers = {"user-agent": "Mozilla/5.0", "referer": f"https://www.flashscore.com.br/jogo/{match_id}/"}
    
    try:
        r = session.get(url, params=params, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json().get("data", {}).get("findEventSummaryByEventId", {})
            
            # Mapeamento de Rótulos para Colunas
            mapa_stats = {
                "xG": "xG", "Gols Esperados": "xG",
                "Posse": "Posse", "bola": "Posse",
                "Finalizações": "Chutes",
                "alvo": "Chutes_Alvo",
                "clara": "Chances_Claras",
                "dentro da área": "Toques_Area",
                "Escanteios": "Escanteios",
                "Defesas": "Defesas"
            }

            stats_list = data.get("stats", [])
            for p_stats in stats_list:
                period_stats = p_stats.get("stats", [])
                p_type = p_stats.get("periodType", "ALL")
                prefix = "FT" if p_type == "ALL" else "HT" if p_type == "FIRST_HALF" else None
                if not prefix: continue

                for s in period_stats:
                    lbl = s.get("label", "")
                    h, a = s.get("homeValue"), s.get("awayValue")
                    
                    for chave, col in mapa_stats.items():
                        if chave.lower() in lbl.lower():
                            detalhes[f"{prefix}_{col}_Home"] = limpar_valor(h)
                            detalhes[f"{prefix}_{col}_Away"] = limpar_valor(a)
                            break

            periods = data.get("periods", [])
            for p in periods:
                pt = p.get("periodType")
                if pt == "FIRST_HALF":
                    detalhes["Gols_Home_HT"], detalhes["Gols_Away_HT"] = limpar_valor(p.get("homeScore")), limpar_valor(p.get("awayScore"))
                elif pt == "ALL":
                    detalhes["Gols_Home_FT"], detalhes["Gols_Away_FT"] = limpar_valor(p.get("homeScore")), limpar_valor(p.get("awayScore"))
    except Exception as e:
        log.debug(f"Erro Ninja Stats ({match_id}): {e}")
    return detalhes

def salvar_no_banco(jogo, conn):
    """Insere ou substitui dados no SQLite."""
    cursor = conn.execute("PRAGMA table_info(jogos_flashscore)")
    cols_existentes = [row[1] for row in cursor.fetchall()]
    jogo_filtrado = {k: v for k, v in jogo.items() if k in cols_existentes}
    
    cols = list(jogo_filtrado.keys())
    sql = f"INSERT OR REPLACE INTO jogos_flashscore ({', '.join(cols)}) VALUES ({':' + ', :'.join(cols)})"
    conn.execute(sql, jogo_filtrado)

def processar_jogo(jogo, session, conn):
    """Processa um jogo e salva no banco."""
    detalhes = coletar_detalhes_ninja(jogo['Match_ID'], session)
    odds = extrair_odds_ninja(jogo['Match_ID'], session)
    
    jogo.update(detalhes)
    jogo.update(odds)

    if jogo.get("Gols_Home_FT") is not None or jogo.get("Odd_H_Open") is not None:
        salvar_no_banco(jogo, conn)
    return jogo

def coletar_liga_flashscore(driver, session, cfg, max_jogos=None):
    log.info(f"Iniciando: {cfg['pais']} - {cfg['liga']}")
    inicializar_banco()
    driver.get(cfg['url'])

    while True:
        try:
            btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Mostrar mais jogos')] | //a[contains(@class, 'event__more')]")))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(2)
            if max_jogos and max_jogos <= 50: break 
        except: break

    jogos_raw = driver.execute_script("""
        const rows = document.querySelectorAll('.event__match');
        const data = [];
        rows.forEach(row => {
            const mid = row.id.split('_').pop();
            const home = row.querySelector("[class*='homeParticipant']")?.innerText.trim();
            const away = row.querySelector("[class*='awayParticipant']")?.innerText.trim();
            const timeStr = row.querySelector("[class*='time']")?.innerText.trim() || "";
            if (mid && home) data.push({ "Match_ID": mid, "Home": home, "Away": away, "Time": timeStr });
        });
        return data;
    """)

    jogos = []
    for j in jogos_raw:
        dt, hr = "", ""
        if "." in j["Time"]:
            parts = j["Time"].split(" ")
            dt, hr = parts[0].replace(".", "-"), (parts[1] if len(parts) > 1 else "")
        jogos.append({
            "Match_ID": j["Match_ID"], "Data": dt, "Hora": hr, "Pais": cfg['pais'], "Liga": cfg['liga'],
            "Temporada": cfg['temporada'], "Rodada": "", "Home": j["Home"], "Away": j["Away"]
        })

    if max_jogos:
        jogos = jogos[:max_jogos]

    with sqlite3.connect(DB_NAME, timeout=30) as conn:
        cursor = conn.execute("SELECT Match_ID FROM jogos_flashscore WHERE FT_xG_Home IS NOT NULL OR Odd_H_Open IS NOT NULL")
        ids_completos = [row[0] for row in cursor.fetchall()]
        
        pendentes = [j for j in jogos if j['Match_ID'] not in ids_completos]
        log.info(f"Pendentes: {len(pendentes)} de {len(jogos)}")

        for jogo in tqdm(pendentes, desc=f"Lendo {cfg['liga']}"):
            processar_jogo(jogo, session, conn)
            conn.commit() 
            time.sleep(0.05)

    with sqlite3.connect(DB_NAME, timeout=30) as conn:
        df = pd.read_sql("SELECT * FROM jogos_flashscore", conn)
        df.to_csv("dados_flashscore_final.csv", index=False)
    log.info(f"Coleta concluída para {cfg['liga']}!")

def main():
    TEST_MODE = True 
    TEST_LEAGUE = "Brasil - Serie A"
    TEST_LIMIT = None

    CONFIG_FLASH = {
        "Alemanha - Bundesliga": {"url": "https://www.flashscore.com.br/futebol/alemanha/bundesliga-2024-2025/resultados/", "pais": "Alemanha", "liga": "Bundesliga", "temporada": "2024-2025"},
        "Alemanha - 2. Bundesliga": {"url": "https://www.flashscore.com.br/futebol/alemanha/2-bundesliga-2024-2025/resultados/", "pais": "Alemanha", "liga": "2. Bundesliga", "temporada": "2024-2025"},
        "Brasil - Serie A": {"url": "https://www.flashscore.com.br/futebol/brasil/brasileirao-betano-2024/resultados/", "pais": "Brasil", "liga": "Série A", "temporada": "2024"},
        "Espanha - La Liga": {"url": "https://www.flashscore.com.br/futebol/espanha/laliga-2024-2025/resultados/", "pais": "Espanha", "liga": "LaLiga", "temporada": "2024-2025"},
        "Inglaterra - Premier League": {"url": "https://www.flashscore.com.br/futebol/inglaterra/premier-league-2024-2025/resultados/", "pais": "Inglaterra", "liga": "Premier League", "temporada": "2024-2025"},
        "Inglaterra - Championship": {"url": "https://www.flashscore.com.br/futebol/inglaterra/championship-2024-2025/resultados/", "pais": "Inglaterra", "liga": "Championship", "temporada": "2024-2025"},
        "Inglaterra - League One": {"url": "https://www.flashscore.com.br/futebol/inglaterra/league-one-2024-2025/resultados/", "pais": "Inglaterra", "liga": "League One", "temporada": "2024-2025"},
        "Inglaterra - League Two": {"url": "https://www.flashscore.com.br/futebol/inglaterra/league-two-2024-2025/resultados/", "pais": "Inglaterra", "liga": "League Two", "temporada": "2024-2025"},
        "França - Ligue 1": {"url": "https://www.flashscore.com.br/futebol/franca/ligue-1-2024-2025/resultados/", "pais": "França", "liga": "Ligue 1", "temporada": "2024-2025"},
        "Itália - Série A": {"url": "https://www.flashscore.com.br/futebol/italia/serie-a-2024-2025/resultados/", "pais": "Itália", "liga": "Série A", "temporada": "2024-2025"},
        "Portugal - Primeira Liga": {"url": "https://www.flashscore.com.br/futebol/portugal/liga-portugal-2024-2025/resultados/", "pais": "Portugal", "liga": "Liga Portugal", "temporada": "2024-2025"},
        "Países Baixos - Eredivisie": {"url": "https://www.flashscore.com.br/futebol/holanda/eredivisie-2024-2025/resultados/", "pais": "Países Baixos", "liga": "Eredivisie", "temporada": "2024-2025"},
        "Bélgica - Pro League": {"url": "https://www.flashscore.com.br/futebol/belgica/liga-jupiler-2024-2025/resultados/", "pais": "Bélgica", "liga": "Liga Jupiler", "temporada": "2024-2025"},
        "Escócia - Premiership": {"url": "https://www.flashscore.com.br/futebol/escocia/premiership-2024-2025/resultados/", "pais": "Escócia", "liga": "Premiership", "temporada": "2024-2025"},
        "Áustria - Tipico Bundesliga": {"url": "https://www.flashscore.com.br/futebol/austria/bundesliga-2024-2025/resultados/", "pais": "Áustria", "liga": "Bundesliga", "temporada": "2024-2025"}
    }
    
    session = requests.Session()
    session.headers.update({"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", "referer": "https://www.flashscore.com.br/"})
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    ligas_lista = [TEST_LEAGUE] if TEST_MODE else [l for l in LIGAS_XG if l in CONFIG_FLASH]
    limit = TEST_LIMIT if TEST_MODE else None

    for nome in ligas_lista:
        driver = webdriver.Chrome(options=options)
        try: 
            coletar_liga_flashscore(driver, session, CONFIG_FLASH[nome], max_jogos=limit)
        finally: 
            driver.quit()

if __name__ == "__main__":
    main()
