import time
import logging
import sqlite3
import pandas as pd
import requests
import re
import json
import os
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
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
    s_val = str(val).replace("%", "").strip()
    limpo = "".join(c for c in s_val if c.isdigit() or c == "." or c == ",")
    limpo = limpo.replace(",", ".")
    try:
        if "." in limpo: return float(limpo)
        return int(limpo)
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
            HT_xG_Home REAL, HT_xG_Away REAL, HT_xGOT_Home REAL, HT_xGOT_Away REAL, HT_Posse_Home REAL, HT_Posse_Away REAL,
            HT_Chutes_Home INTEGER, HT_Chutes_Away INTEGER, HT_Chutes_Alvo_Home INTEGER, HT_Chutes_Alvo_Away INTEGER,
            HT_Chances_Claras_Home INTEGER, HT_Chances_Claras_Away INTEGER, HT_Toques_Area_Home INTEGER, HT_Toques_Area_Away INTEGER,
            HT_Escanteios_Home INTEGER, HT_Escanteios_Away INTEGER, HT_Defesas_Home INTEGER, HT_Defesas_Away INTEGER
        )
        """)
        conn.commit()

def extrair_odds_ninja(match_id, session):
    res = {}
    url = "https://global.ds.lsapp.eu/odds/pq_graphql"
    base_params = {"eventId": match_id, "projectId": "401", "geoIpCode": "BR", "geoIpSubdivisionCode": "BR"}
    headers = {"user-agent": "Mozilla/5.0", "referer": f"https://www.flashscore.com.br/jogo/{match_id}/", "Origin": "https://www.flashscore.com.br"}

    for h in ["pobtm", "oce"]:
        try:
            params = base_params.copy()
            params.update({"_hash": h})
            r = session.get(url, params=params, headers=headers, timeout=8)
            if r.status_code == 200:
                data = r.json().get("data", {}).get("findOddsByEventId", {}).get("odds", [])
                for market in data:
                    bt = market.get("bettingType")
                    pt = market.get("periodType", "ALL")
                    odds = market.get("odds", [])
                    if not odds: continue

                    if bt == "HOME_DRAW_AWAY" and pt == "ALL" and len(odds) >= 3:
                        res["Odd_H_Open"] = res.get("Odd_H_Open") or limpar_valor(odds[0].get("opening"))
                        res["Odd_H_Close"] = res.get("Odd_H_Close") or limpar_valor(odds[0].get("value"))
                        res["Odd_D_Open"] = res.get("Odd_D_Open") or limpar_valor(odds[1].get("opening"))
                        res["Odd_D_Close"] = res.get("Odd_D_Close") or limpar_valor(odds[1].get("value"))
                        res["Odd_A_Open"] = res.get("Odd_A_Open") or limpar_valor(odds[2].get("opening"))
                        res["Odd_A_Close"] = res.get("Odd_A_Close") or limpar_valor(odds[2].get("value"))
                    
                    elif bt == "DOUBLE_CHANCE" and pt == "ALL" and len(odds) >= 3:
                        res["DC_1X_Open"] = res.get("DC_1X_Open") or limpar_valor(odds[0].get("opening"))
                        res["DC_1X_Close"] = res.get("DC_1X_Close") or limpar_valor(odds[0].get("value"))
                        res["DC_12_Open"] = res.get("DC_12_Open") or limpar_valor(odds[1].get("opening"))
                        res["DC_12_Close"] = res.get("DC_12_Close") or limpar_valor(odds[1].get("value"))
                        res["DC_X2_Open"] = res.get("DC_X2_Open") or limpar_valor(odds[2].get("opening"))
                        res["DC_X2_Close"] = res.get("DC_X2_Close") or limpar_valor(odds[2].get("value"))

                    elif bt == "OVER_UNDER":
                        for row in odds:
                            n = str(row.get("name"))
                            if n == "2.5" and pt == "ALL":
                                res["Over_25_Open"] = res.get("Over_25_Open") or limpar_valor(row.get("over", {}).get("opening"))
                                res["Over_25_Close"] = res.get("Over_25_Close") or limpar_valor(row.get("over", {}).get("value"))
                                res["Under_25_Open"] = res.get("Under_25_Open") or limpar_valor(row.get("under", {}).get("opening"))
                                res["Under_25_Close"] = res.get("Under_25_Close") or limpar_valor(row.get("under", {}).get("value"))
        except: continue
    return res

def coletar_detalhes_ninja(match_id, session):
    detalhes = {}
    url = "https://global.ds.lsapp.eu/pq_graphql"
    params = {"_hash": "dsos2", "eventId": match_id, "projectId": "401"}
    headers = {"user-agent": "Mozilla/5.0", "referer": f"https://www.flashscore.com.br/jogo/{match_id}/"}
    
    try:
        r = session.get(url, params=params, headers=headers, timeout=8)
        if r.status_code == 200:
            json_data = r.json().get("data", {})
            event_data = json_data.get("findEventById", {}) or json_data.get("findEventSummaryByEventId", {})
            
            mapa_stats = {
                "expected_goals": "xG", "expected_goals_on_target": "xGOT", "ball_possession": "Posse",
                "goal_attempts": "Chutes", "shots_on_goal": "Chutes_Alvo", "big_chances": "Chances_Claras",
                "touches_in_opposition_box": "Toques_Area", "corner_kicks": "Escanteios", "goalkeeper_saves": "Defesas"
            }

            for part in event_data.get("eventParticipants", []):
                side = part.get("type", {}).get("side")
                if side not in ["HOME", "AWAY"]: continue
                suffix = "Home" if side == "HOME" else "Away"
                for sp in part.get("stats", []):
                    ptype = sp.get("periodType", "ALL")
                    prefix = "FT" if ptype == "ALL" else "HT" if ptype == "FIRST_HALF" else None
                    if not prefix: continue
                    for entry in sp.get("values", []):
                        etype = entry.get("type")
                        if etype in mapa_stats:
                            detalhes[f"{prefix}_{mapa_stats[etype]}_{suffix}"] = limpar_valor(entry.get("value"))
    except: pass
    return detalhes

def salvar_no_banco(jogo, conn):
    cursor = conn.execute("PRAGMA table_info(jogos_flashscore)")
    cols_existentes = [row[1] for row in cursor.fetchall()]
    jogo_filtrado = {k: v for k, v in jogo.items() if k in cols_existentes}
    cols = list(jogo_filtrado.keys())
    sql = f"INSERT OR REPLACE INTO jogos_flashscore ({', '.join(cols)}) VALUES ({':' + ', :'.join(cols)})"
    conn.execute(sql, jogo_filtrado)

def processar_jogo(jogo, session, conn):
    detalhes = coletar_detalhes_ninja(jogo['Match_ID'], session)
    time.sleep(random.uniform(0.1, 0.2))
    odds = extrair_odds_ninja(jogo['Match_ID'], session)
    jogo.update(detalhes)
    jogo.update(odds)
    salvar_no_banco(jogo, conn)

def coletar_liga_flashscore(driver, session, cfg, max_jogos=None):
    log.info(f"Iniciando: {cfg['pais']} - {cfg['liga']}")
    inicializar_banco()
    
    driver.get(cfg['url'])
    time.sleep(5)
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])
    
    # Expandir lista
    for _ in range(10): # Limite de tentativas de expansão
        try:
            btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Mostrar mais jogos')] | //a[contains(@class, 'event__more')]")))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(2)
            if max_jogos and max_jogos <= 50: break 
        except: break

    # Extração JS Robusta
    jogos_raw = driver.execute_script("""
        const items = document.querySelectorAll('.event__match, .event__round');
        const data = [];
        let currentRound = "";
        items.forEach(item => {
            if (item.classList.contains('event__round')) {
                currentRound = item.innerText.trim();
            } else {
                const mid = item.id.split('_').pop();
                const home = item.querySelector("[class*='homeParticipant']")?.innerText.trim();
                const away = item.querySelector("[class*='awayParticipant']")?.innerText.trim();
                const timeStr = item.querySelector("[class*='time']")?.innerText.trim() || "";
                const scoreHome = item.querySelector(".event__score--home")?.innerText.trim() || "";
                const scoreAway = item.querySelector(".event__score--away")?.innerText.trim() || "";
                const partHT = item.querySelector(".event__part")?.innerText.trim() || "";
                
                let gh_ht = null, ga_ht = null;
                if (partHT.includes('-')) {
                    const cleanHT = partHT.replace('(', '').replace(')', '').split('-');
                    gh_ht = cleanHT[0]?.trim(); ga_ht = cleanHT[1]?.trim();
                }
                if (mid && home) {
                    data.push({ 
                        "Match_ID": mid, "Home": home, "Away": away, "Time": timeStr, "Rodada": currentRound,
                        "GH_FT": scoreHome, "GA_FT": scoreAway, "GH_HT": gh_ht, "GA_HT": ga_ht
                    });
                }
            }
        });
        return data;
    """)

    if not jogos_raw:
        log.warning(f"Nenhum jogo encontrado para {cfg['liga']}. Verifique a URL.")
        return

    jogos = []
    for j in jogos_raw:
        dt, hr = "", ""
        if "." in j["Time"]:
            parts = j["Time"].split(" ")
            dt = parts[0].replace(".", "-")
            hr = parts[1] if len(parts) > 1 else ""
        
        jogos.append({
            "Match_ID": j["Match_ID"], "Data": dt, "Hora": hr, "Pais": cfg['pais'], "Liga": cfg['liga'],
            "Temporada": cfg['temporada'], "Rodada": j["Rodada"], "Home": j["Home"], "Away": j["Away"],
            "Gols_Home_FT": limpar_valor(j["GH_FT"]), "Gols_Away_FT": limpar_valor(j["GA_FT"]),
            "Gols_Home_HT": limpar_valor(j["GH_HT"]), "Gols_Away_HT": limpar_valor(j["GA_HT"])
        })

    if max_jogos: jogos = jogos[:max_jogos]

    with sqlite3.connect(DB_NAME, timeout=60) as conn:
        cursor = conn.execute("SELECT Match_ID FROM jogos_flashscore WHERE FT_xG_Home IS NOT NULL")
        ids_completos = [row[0] for row in cursor.fetchall()]
        pendentes = [j for j in jogos if j['Match_ID'] not in ids_completos]
        log.info(f"Pendentes: {len(pendentes)} de {len(jogos)}")

        for jogo in tqdm(pendentes, desc=f"Lendo {cfg['liga']}"):
            processar_jogo(jogo, session, conn)
            conn.commit() 
            time.sleep(random.uniform(0.1, 0.3))

    with sqlite3.connect(DB_NAME, timeout=60) as conn:
        df = pd.read_sql("SELECT * FROM jogos_flashscore", conn)
        df.to_csv("dados_flashscore_final.csv", index=False)
    log.info(f"Sucesso: {cfg['liga']}")

def main():
    # CONFIGURAÇÃO DE TESTE
    TEST_MODE = True
    TEST_LEAGUE = "Brasil - Serie A"
    TEST_LIMIT = None

    CONFIG_FLASH = {
        "Brasil - Serie A": {"url": "https://www.flashscore.com.br/futebol/brasil/brasileirao-betano-2024/resultados/", "pais": "Brasil", "liga": "Série A", "temporada": "2024"},
        "Espanha - La Liga": {"url": "https://www.flashscore.com.br/futebol/espanha/laliga-2024-2025/resultados/", "pais": "Espanha", "liga": "LaLiga", "temporada": "2024-2025"},
        "Itália - Série A": {"url": "https://www.flashscore.com.br/futebol/italia/serie-a-2024-2025/resultados/", "pais": "Itália", "liga": "Série A", "temporada": "2024-2025"}
    }
    
    session = requests.Session()
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    ligas_lista = [TEST_LEAGUE] if TEST_MODE else [l for l in LIGAS_XG if l in CONFIG_FLASH]
    
    for nome in ligas_lista:
        if nome not in CONFIG_FLASH: continue
        driver = webdriver.Chrome(options=options)
        try: 
            coletar_liga_flashscore(driver, session, CONFIG_FLASH[nome], max_jogos=TEST_LIMIT)
        except Exception as e:
            log.error(f"Erro crítico na liga {nome}: {e}")
        finally: 
            driver.quit()

if __name__ == "__main__":
    main()
