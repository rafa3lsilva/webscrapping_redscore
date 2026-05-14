"""
Flashscore Collector v2 — Reproduz o esquema do CSV padrão-ouro.

Usa o endpoint `df_st_1_{match_id}` que retorna TODAS as estatísticas
(xG, xGOT, xA, Passes%, Tackles%, etc.) nos 3 períodos (FT, HT, 2T).

Alinhado com a Skill §4: interceptação de requests → JSON/raw → parser → DB
"""
import time
import logging
import sqlite3
import pandas as pd
import requests
import re
import json
import os
import sys
import random
from datetime import date

# Permite importar módulos do projeto raiz
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from config.leagues import LIGAS_FLASHSCORE

# ============================================================
# Configuração
# ============================================================
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(os.path.join(_SCRIPT_DIR, "coletor_flashscore.log"), encoding="utf-8"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DB_NAME = os.path.join(_SCRIPT_DIR, "flashscore_v2.db")
CSV_NAME = os.path.join(_SCRIPT_DIR, "flashscore_v2.csv")
RAW_DIR = os.path.join(_SCRIPT_DIR, "raw")

# ============================================================
# Mapeamento de Stat_ID → Nome da coluna no CSV padrão-ouro
# ============================================================
STAT_ID_MAP = {
    '432': 'xG',
    '499': 'xGOT',
    '503': 'xA',
    '501': 'xGOT_Faced',
    '511': 'Goals_Prevented',
    '12':  'Possession',
    '342': 'Passes_Pct',
    '517': 'Long_Passes_Pct',
    '467': 'Passes_Final_Third_Pct',
    '521': 'Through_Passes',
    '433': 'Crosses_Pct',
    '34':  'Total_Shots',
    '13':  'Shots_On_Target',
    '14':  'Shots_Off_Target',
    '158': 'Blocked_Shots',
    '461': 'Shots_Inside_Box',
    '463': 'Shots_Outside_Box',
    '459': 'Big_Chances',
    '457': 'Hit_Woodwork',
    '471': 'Touches_Box',
    '16':  'Corners',
    '15':  'Free_Kicks',
    '18':  'Throw_Ins',
    '21':  'Fouls',
    '17':  'Offsides',
    '19':  'Goalkeeper_Saves',
    '475': 'Tackles_Pct',
    '513': 'Duels_Won',
    '479': 'Clearances',
    '434': 'Interceptions',
    '507': 'Errors_Shot',
    '509': 'Errors_Goal',
}

PERIOD_MAP = {
    'Jogo': 'FT',
    '1º tempo': 'HT',
    '1. tempo': 'HT',
    '2º tempo': '2T',
    '2. tempo': '2T',
}

def parse_value(raw: str) -> float | int | None:
    if not raw or raw.strip() == '': return None
    raw = raw.strip()
    m = re.match(r'^(\d+)%', raw)
    if m: return round(int(m.group(1)) / 100, 2)
    clean = raw.replace(',', '.')
    try:
        f = float(clean)
        return int(f) if f == int(f) and '.' not in clean else f
    except: return None

def parse_stats_feed(text: str) -> dict:
    result = {}
    current_period = 'FT'
    seen = set()
    sections = text.split('¬~')
    for s in sections:
        fields = {}
        for pair in s.split('¬'):
            if '÷' in pair:
                key, val = pair.split('÷', 1)
                fields[key] = val
        if 'SE' in fields:
            current_period = PERIOD_MAP.get(fields['SE'], 'FT')
        if 'SD' in fields and 'SG' in fields:
            stat_id = fields['SD']
            col_name = STAT_ID_MAP.get(stat_id)
            if not col_name: continue
            key = f'{current_period}|{stat_id}'
            if key in seen: continue
            seen.add(key)
            result[f'{col_name}_Home_{current_period}'] = parse_value(fields.get('SH', ''))
            result[f'{col_name}_Away_{current_period}'] = parse_value(fields.get('SI', ''))
    return result

# ============================================================
# Coleta de dados via HTTP
# ============================================================
BASE_URL = "https://www.flashscore.com.br"
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-fsign': 'SW9D1eZo',
}

def fetch_stats(match_id: str, driver: webdriver.Chrome, session: requests.Session) -> dict:
    """Busca estatísticas e metadados (Hora, HT) combinando API e Selenium."""
    detalhes = {}
    
    # 1. A hora exata está na API de detalhes gerais (dc_1) (Muito rápido e 100% preciso)
    url_details = f'{BASE_URL}/x/feed/dc_1_{match_id}'
    try:
        r_d = session.get(url_details, headers={**HEADERS, 'referer': f'{BASE_URL}/jogo/{match_id}/'}, timeout=10)
        if r_d.status_code == 200:
            fields = {}
            for pair in r_d.text.split('¬'):
                if '÷' in pair:
                    k, v = pair.split('÷', 1)
                    fields[k] = v
            # DC = Timestamp da partida
            if 'DC' in fields:
                from datetime import datetime
                ts = int(fields['DC'])
                detalhes['Time'] = datetime.fromtimestamp(ts).strftime('%H:%M')
    except:
        log.warning(f"Não foi possível capturar a hora via API para o jogo {match_id}")

    # 2. O Placar HT exato só existe no HTML (as APIs copiam o placar FT em jogos encerrados)
    try:
        jogo_url = f'{BASE_URL}/jogo/{match_id}/#/resumo-de-jogo/estatisticas-de-jogo'
        if driver.current_url != jogo_url:
            driver.get(jogo_url)
            time.sleep(1)
        
        # O Flashscore renderiza a timeline com '1º TEMPO\nHome - Away' na tela
        ht_score = driver.execute_script('''
            const text = document.querySelector("#detail")?.innerText || "";
            const match = text.match(/1º TEMPO\\n(\\d+)\\s*-\\s*(\\d+)/i);
            if (match) return [parseInt(match[1]), parseInt(match[2])];
            return null;
        ''')
        if ht_score:
            detalhes['Home_Score_HT'] = ht_score[0]
            detalhes['Away_Score_HT'] = ht_score[1]
    except:
        log.warning(f"Não foi possível extrair o placar HT para o jogo {match_id}")

    # 3. Buscar Estatísticas detalhadas via Feed (df_st_1)
    url_stats = f'{BASE_URL}/x/feed/df_st_1_{match_id}'
    try:
        r = session.get(url_stats, headers={**HEADERS, 'referer': f'{BASE_URL}/jogo/{match_id}/'}, timeout=12)
        if r.status_code == 200 and len(r.text) > 5:
            stats_data = parse_stats_feed(r.text)
            detalhes.update(stats_data)
    except: pass
    
    return detalhes

def fetch_odds_graphql(match_id: str, session: requests.Session) -> dict:
    res = {}
    url = "https://global.ds.lsapp.eu/odds/pq_graphql"
    base_params = {"eventId": match_id, "projectId": "401", "geoIpCode": "BR", "geoIpSubdivisionCode": "BR"}
    headers = {"user-agent": "Mozilla/5.0", "referer": f"{BASE_URL}/jogo/{match_id}/", "Origin": BASE_URL}

    for h in ["pobtm", "oce"]:
        try:
            params = {**base_params, "_hash": h}
            r = session.get(url, params=params, headers=headers, timeout=8)
            if r.status_code != 200: continue
            data = r.json().get("data", {}).get("findOddsByEventId", {}).get("odds", [])
            for market in data:
                bt = market.get("bettingType")
                pt = market.get("bettingScope", "FULL_TIME")
                odds = market.get("odds", [])
                if bt == "HOME_DRAW_AWAY" and pt == "FULL_TIME" and len(odds) >= 3:
                    if not res.get("Odd_H_FT"):
                        res["Odd_H_FT"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["Odd_D_FT"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                        res["Odd_A_FT"] = parse_value(str(odds[2].get("opening") or odds[2].get("value")))
                elif bt == "HOME_DRAW_AWAY" and pt == "FIRST_HALF" and len(odds) >= 3:
                    if not res.get("Odd_H_HT"):
                        res["Odd_H_HT"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["Odd_D_HT"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                        res["Odd_A_HT"] = parse_value(str(odds[2].get("opening") or odds[2].get("value")))
                elif bt == "BOTH_TEAMS_TO_SCORE" and pt == "ALL":
                    if not res.get("BTTS_Yes") and len(odds) >= 2:
                        res["BTTS_Yes"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["BTTS_No"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                elif bt == "DOUBLE_CHANCE" and pt == "ALL":
                    if not res.get("DC_1X") and len(odds) >= 3:
                        res["DC_1X"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["DC_12"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                        res["DC_X2"] = parse_value(str(odds[2].get("opening") or odds[2].get("value")))
                elif bt == "OVER_UNDER":
                    grouped = {}
                    prefix = "FT" if pt == "FULL_TIME" else "HT"
                    for o in odds:
                        h_val = str(o.get("handicap", {}).get("value", ""))
                        sel = o.get("selection")
                        val = parse_value(str(o.get("opening") or o.get("value")))
                        # Filter to only keep exact .5 lines
                        if h_val and h_val.endswith(".5") and sel and val is not None:
                            if h_val not in grouped: grouped[h_val] = {}
                            grouped[h_val][sel] = val
                    
                    for h_val, sels in grouped.items():
                        if "OVER" in sels and "UNDER" in sels:
                            res[f"Over_{prefix}_{h_val}"] = sels["OVER"]
                            res[f"Under_{prefix}_{h_val}"] = sels["UNDER"]
        except: pass
    return res

def reorder_csv_columns(df):
    meta = ['Match_ID', 'Country', 'Season', 'Div', 'League', 'Date', 'Time', 'Round', 'Home', 'Away']
    goals = ['Home_Score', 'Away_Score', 'Home_Score_HT', 'Away_Score_HT']
    
    odds_p1 = ['Odd_H_FT', 'Odd_D_FT', 'Odd_A_FT', 'Odd_H_HT', 'Odd_D_HT', 'Odd_A_HT', 
               'Over_FT_2.5', 'Under_FT_2.5', 'BTTS_Yes', 'BTTS_No', 'Over_HT_0.5', 'Under_HT_0.5']
               
    odds_p2 = ['Over_FT_0.5', 'Under_FT_0.5', 'Over_FT_1.5', 'Under_FT_1.5', 
               'Over_FT_3.5', 'Under_FT_3.5', 'Over_FT_4.5', 'Under_FT_4.5',
               'Over_HT_1.5', 'Under_HT_1.5', 'Over_HT_2.5', 'Under_HT_2.5']
               
    odds_p3 = ['DC_1X', 'DC_12', 'DC_X2']
    
    all_cols = list(df.columns)
    
    desired_order = meta + goals + odds_p1 + odds_p2 + odds_p3
    final_order = [c for c in desired_order if c in all_cols]
    
    remaining = sorted([c for c in all_cols if c not in final_order and c != 'stats_collected'])
    final_order.extend(remaining)
    
    if 'stats_collected' in all_cols:
        final_order.append('stats_collected')
        
    return df[final_order]

# ============================================================
# Banco de Dados
# ============================================================
def inicializar_banco():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jogos (
            Match_ID TEXT PRIMARY KEY, Country TEXT, Season TEXT, Div TEXT, League TEXT,
            Date TEXT, Time TEXT, Round TEXT, Home TEXT, Away TEXT,
            Home_Score INTEGER, Away_Score INTEGER, Home_Score_HT INTEGER, Away_Score_HT INTEGER,
            stats_collected INTEGER DEFAULT 0
        )""")
        conn.commit()

def salvar_jogo(conn, jogo: dict):
    cursor = conn.execute("PRAGMA table_info(jogos)")
    existing = {row[1] for row in cursor.fetchall()}
    for col in jogo:
        if col not in existing:
            col_type = "REAL" if isinstance(jogo[col], (int, float)) else "TEXT"
            try: conn.execute(f'ALTER TABLE jogos ADD COLUMN "{col}" {col_type}')
            except: pass
    cols = list(jogo.keys())
    placeholders = ', '.join(['?'] * len(cols))
    col_names = ', '.join([f'"{c}"' for c in cols])
    sql = f'INSERT OR REPLACE INTO jogos ({col_names}) VALUES ({placeholders})'
    conn.execute(sql, [jogo[c] for c in cols])

def processar_jogo(jogo: dict, driver: webdriver.Chrome, session: requests.Session, conn) -> bool:
    match_id = jogo['Match_ID']
    stats = fetch_stats(match_id, driver, session)
    jogo.update(stats)
    time.sleep(random.uniform(0.1, 0.2))
    odds = fetch_odds_graphql(match_id, session)
    jogo.update(odds)
    jogo['stats_collected'] = 1 if stats else 0
    salvar_jogo(conn, jogo)
    return bool(stats)

def coletar_liga(driver, session, cfg, max_jogos=None):
    log.info(f"Iniciando: {cfg['pais']} - {cfg['liga']}")
    inicializar_banco()
    driver.get(cfg['url'])
    time.sleep(5)
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])

    expansions = 0
    for _ in range(60):
        try:
            btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Mostrar mais jogos')] | //a[contains(@class, 'event__more')]")))
            driver.execute_script("arguments[0].click();", btn)
            expansions += 1
            time.sleep(2)
        except: break
    log.info(f"Expansão: {expansions} cliques em 'Mostrar mais jogos'")

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
                if (mid && home) {
                    data.push({mid, home, away, timeStr, scoreHome, scoreAway, partHT, currentRound});
                }
            }
        });
        return data;
    """)

    if not jogos_raw: return
    ano_temp = cfg['temporada']
    jogos = []
    for j in jogos_raw:
        dt_str, hr = "", ""
        raw_time = j.get("timeStr", "").strip()
        if raw_time:
            parts = raw_time.split(" ")
            date_part = parts[0].rstrip(".")
            hr = parts[1] if len(parts) > 1 else ""
            date_segs = date_part.split(".")
            if len(date_segs) == 2:
                dt_str = f"{date_segs[0].zfill(2)}/{date_segs[1].zfill(2)}/{ano_temp}"
            elif len(date_segs) == 3:
                dt_str = f"{date_segs[0].zfill(2)}/{date_segs[1].zfill(2)}/{date_segs[2]}"
            else: dt_str = date_part

        ht_score = j.get("partHT", "").replace("(", "").replace(")", "")
        gh_ht, ga_ht = None, None
        if "-" in ht_score:
            p = ht_score.split("-")
            gh_ht = parse_value(p[0].strip()); ga_ht = parse_value(p[1].strip())

        jogos.append({
            "Match_ID": j["mid"], "Country": cfg['pais'].upper(), "Season": cfg['temporada'],
            "Div": cfg['div'], "League": cfg['league_code'], "Date": dt_str, "Time": hr,
            "Round": j["currentRound"], "Home": j["home"], "Away": j["away"],
            "Home_Score": parse_value(j["scoreHome"]), "Away_Score": parse_value(j["scoreAway"]),
            "Home_Score_HT": gh_ht, "Away_Score_HT": ga_ht
        })

    if max_jogos: jogos = jogos[:max_jogos]
    with sqlite3.connect(DB_NAME, timeout=60) as conn:
        try:
            cursor = conn.execute("SELECT Match_ID FROM jogos WHERE stats_collected = 1")
            ids_completos = {row[0] for row in cursor.fetchall()}
        except: ids_completos = set()
        pendentes = [j for j in jogos if j['Match_ID'] not in ids_completos]
        log.info(f"Total: {len(jogos)} | Já coletados: {len(ids_completos)} | Pendentes: {len(pendentes)}")
        for jogo in tqdm(pendentes, desc=f"📊 {cfg['liga']}"):
            processar_jogo(jogo, driver, session, conn)
            conn.commit()
            time.sleep(random.uniform(0.2, 0.5))
    with sqlite3.connect(DB_NAME, timeout=60) as conn:
        df = pd.read_sql("SELECT * FROM jogos", conn)
        if not df.empty:
            df = reorder_csv_columns(df)
            df.to_csv(CSV_NAME, index=False)
    log.info(f"✅ Sucesso. CSV: {CSV_NAME}")

def main():
    session = requests.Session()
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        for nome_liga, conf in LIGAS_FLASHSCORE.items():
            temporadas = conf.get("temporadas", [])
            for idx, temp in enumerate(temporadas):
                # A primeira temporada da lista é considerada a atual (URL sem ano)
                is_current = (idx == 0)
                url_base = conf["url_base"].rstrip("/")
                
                if is_current:
                    url = f"{url_base}/resultados/"
                else:
                    # Flashscore usa formato ano (2024) ou duplo ano (2023-2024)
                    url = f"{url_base}-{temp}/resultados/"
                
                cfg = {
                    "url": url,
                    "pais": conf["pais"],
                    "liga": conf["liga"],
                    "temporada": temp,
                    "div": conf["div"],
                    "league_code": conf["league_code"]
                }
                
                log.info(f"--- Iniciando processamento: {nome_liga} ({temp}) ---")
                try: 
                    coletar_liga(driver, session, cfg, max_jogos=None)
                except Exception as e: 
                    log.error(f"Erro ao processar {nome_liga} ({temp}): {e}", exc_info=True)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
