import sys
import time
import requests
import logging
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.leagues import LIGAS_FLASHSCORE
from database.db_manager import save_dict, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.flashscore.com.br"
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-fsign': 'SW9D1eZo',
}

def expandir_jogos(driver: webdriver.Chrome):
    clicks = 0
    while True:
        try:
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Mostrar mais jogos')] | //a[contains(@class, 'event__more')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", btn)
            clicks += 1
            time.sleep(2)
        except:
            break
    log.info(f"Expansão: {clicks} cliques em 'Mostrar mais jogos'")

def parse_value(v):
    try: return int(v)
    except: return None

def coletar_matches(pbar=None):
    init_db()
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    session = requests.Session()
    
    try:
        for cfg_name, cfg in LIGAS_FLASHSCORE.items():
            if not cfg.get("ativo", True): continue
            
            for temp in cfg.get("temporadas", []):
                # Usamos a URL base para a temporada mais recente (primeira da lista) e o sufixo para as antigas
                is_newest = (temp == cfg["temporadas"][0])
                url = cfg["url_base"] if is_newest else f"{cfg['url_base']}-{temp}"
                
                league_name = cfg["liga"]
                log.info(f"Coletando Matches: {cfg_name} ({temp})")
                
                driver.get(f"{url}/resultados/")
                time.sleep(5)
                expandir_jogos(driver)
                
                jogos_raw = driver.execute_script(r"""
                    const items = document.querySelectorAll('.event__match, .event__round');
                    const data = [];
                    let currentRound = "";
                    items.forEach(item => {
                        if (item.classList.contains('event__round')) {
                            currentRound = item.innerText.trim();
                        } else {
                            const mid = item.id.split('_').pop();
                            const home = item.querySelector("[class*='homeParticipant'] [class*='name']")?.innerText.trim() || 
                                         item.querySelector("[class*='homeParticipant']")?.innerText.trim();
                            const away = item.querySelector("[class*='awayParticipant'] [class*='name']")?.innerText.trim() ||
                                         item.querySelector("[class*='awayParticipant']")?.innerText.trim();
                            const timeStr = item.querySelector("[class*='time']")?.innerText.trim() || "";
                            const score = item.querySelector("[class*='score']")?.innerText.trim() || "";
                            const partHT = item.querySelector("[class*='partBottom']")?.innerText.trim() || "";
                            if (mid && home && away) {
                                data.push({ 
                                    Match_ID: mid, 
                                    Round: currentRound, 
                                    timeStr, 
                                    home: home.replace(/\s+/g, ' '), 
                                    away: away.replace(/\s+/g, ' '), 
                                    score, 
                                    partHT 
                                });
                            }
                        }
                    });
                    return data;
                """)
                
                if not jogos_raw: continue
                log.info(f"Encontrados {len(jogos_raw)} jogos na página.")
                
                # Otimização Inteligente: Carregar IDs já finalizados para evitar chamadas de API repetidas
                existing_ids = set()
                from database.db_manager import get_connection
                with get_connection() as conn:
                    league_full_name = f"{cfg.get('div', '')} {temp}"
                    if not is_newest:
                        # Para temporadas passadas, pulamos tudo que já existe
                        cursor = conn.execute("SELECT match_id FROM matches WHERE league_full_name = ?", (league_full_name,))
                    else:
                        # Para a temporada atual, pulamos apenas jogos que já têm placar finalizado (não são nulos)
                        cursor = conn.execute(
                            "SELECT match_id FROM matches WHERE league_full_name = ? AND home_score IS NOT NULL", 
                            (league_full_name,)
                        )
                    existing_ids = {row[0] for row in cursor.fetchall()}
                if existing_ids:
                    log.info(f"Pulando {len(existing_ids)} jogos já consolidados no banco.")

                for j in tqdm(jogos_raw, desc=f"{cfg_name} {temp}"):
                    match_id = j['Match_ID']
                    
                    # Se o jogo já está no banco e possui placar definido (ou é histórico), pulamos
                    if match_id in existing_ids:
                        continue
                    
                    dt_str, hr = "", ""
                    raw_time = j.get("timeStr", "").strip()
                    if raw_time:
                        parts = raw_time.split(" ")
                        date_part = parts[0].rstrip(".")
                        hr = parts[1] if len(parts) > 1 else ""
                        date_segs = date_part.split(".")
                        if len(date_segs) >= 2:
                            dt_str = f"{date_segs[0].zfill(2)}/{date_segs[1].zfill(2)}/{temp}"
                        else: dt_str = date_part

                    score = j.get("score", "")
                    home_ft, away_ft = None, None
                    if "-" in score and "\n" in score:
                        p = score.split("\n-\n")
                        if len(p) == 2:
                            home_ft = parse_value(p[0])
                            away_ft = parse_value(p[1])

                    ht_score = j.get("partHT", "").replace("(", "").replace(")", "")
                    home_ht, away_ht = None, None
                    if "-" in ht_score:
                        p = ht_score.split("-")
                        home_ht = parse_value(p[0])
                        away_ht = parse_value(p[1])

                    # Get accurate scores via dc_1 API (Note: dc_1 copies FT to HT on finished games)
                    url_dc = f"{BASE_URL}/x/feed/dc_1_{match_id}"
                    try:
                        r = session.get(url_dc, headers={**HEADERS, 'referer': f'{BASE_URL}/jogo/{match_id}/'}, timeout=5)
                        if r.status_code == 200:
                            fields = {}
                            for pair in r.text.split('¬'):
                                if '÷' in pair:
                                    k, v = pair.split('÷', 1)
                                    fields[k] = v
                            
                            # DE/DF = FT Scores
                            if 'DE' in fields: home_ft = parse_value(fields['DE'])
                            if 'DF' in fields: away_ft = parse_value(fields['DF'])
                            
                            # Extrair data e hora oficiais via Timestamp (DC)
                            if 'DC' in fields and fields['DC'].isdigit():
                                from datetime import datetime
                                dt_obj = datetime.fromtimestamp(int(fields['DC']))
                                dt_str = dt_obj.strftime('%d/%m/%Y')
                                hr = dt_obj.strftime('%H:%M')
                    except: pass

                    # Extract TRUE HT scores and Goal Minutes from the Match Summary feed
                    import re
                    url_su = f"{BASE_URL}/x/feed/df_su_1_{match_id}"
                    home_goals_minutes, away_goals_minutes = "", ""
                    try:
                        r_su = session.get(url_su, headers={**HEADERS, 'referer': f'{BASE_URL}/jogo/{match_id}/'}, timeout=5)
                        if r_su.status_code == 200:
                            m_ht = re.search(r'AC÷1(?:º|\.) tempo.*?IG÷(\d+).*?IH÷(\d+)', r_su.text)
                            if m_ht:
                                home_ht = int(m_ht.group(1))
                                away_ht = int(m_ht.group(2))
                            
                            # Parse Goal Minutes
                            home_mins, away_mins = [], []
                            blocks = r_su.text.split('~III÷')
                            for b in blocks:
                                # Look for goals (Gol, Pênalti, Gol contra)
                                if 'IK÷Gol' in b or 'IK÷Pênalti' in b:
                                    m_min = re.search(r'IB÷([\d\+\']+)', b)
                                    m_side = re.search(r'IA÷(\d)', b)
                                    if m_min and m_side:
                                        minute = m_min.group(1).replace("'", "")
                                        if m_side.group(1) == '1': home_mins.append(minute)
                                        else: away_mins.append(minute)
                            
                            home_goals_minutes = f"[{', '.join(home_mins)}]"
                            away_goals_minutes = f"[{', '.join(away_mins)}]"
                    except: pass

                    match_data = {
                        "match_id": match_id,
                        "country": cfg.get("pais", ""),
                        "league_full_name": f"{cfg.get('div', '')} {temp}",
                        "league_code": cfg.get("league_code", ""),
                        "season": int(temp.split("-")[0]) if "-" in temp else int(temp),
                        "round": j['Round'],
                        "date": dt_str,
                        "time": hr,
                        "home_team": j['home'],
                        "away_team": j['away'],
                        "home_score": home_ft,
                        "away_score": away_ft,
                        "home_score_ht": home_ht,
                        "away_score_ht": away_ht,
                        "home_goals_minutes": home_goals_minutes,
                        "away_goals_minutes": away_goals_minutes
                    }
                    save_dict("matches", match_data)
                
                if pbar:
                    pbar.update(1)
                    pbar.set_description(f"Matches: {cfg_name} ({temp})")
                    
    finally:
        driver.quit()

if __name__ == "__main__":
    coletar_matches()
