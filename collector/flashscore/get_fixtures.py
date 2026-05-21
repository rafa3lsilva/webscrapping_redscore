import sys
import time
import logging
from pathlib import Path
from datetime import datetime
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

def coletar_fixtures(dias_futuros=7, target_dates=None, pbar=None):
    """
    Coleta as partidas agendadas (calendário) de todas as ligas ativas
    para os próximos X dias ou para datas específicas.
    """
    init_db()
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    
    total_adicionados = 0
    
    try:
        active_leagues = {k: v for k, v in LIGAS_FLASHSCORE.items() if v.get("ativo", True)}
        
        for cfg_name, cfg in active_leagues.items():
            # Pegamos sempre a temporada atual (a primeira da lista)
            if not cfg.get("temporadas"): continue
            temp = cfg["temporadas"][0]
            url = f"{cfg['url_base']}/calendario/"
            
            if pbar:
                pbar.set_description(f"📅 {cfg_name}")
            
            log.debug(f"📅 Acessando Calendário: {cfg_name} ({temp})")
            driver.get(url)
            time.sleep(4)
            
            # Executa script JS para buscar os jogos agendados
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
                        if (mid && home && away) {
                            data.push({ 
                                Match_ID: mid, 
                                Round: currentRound, 
                                timeStr, 
                                home: home.replace(/\s+/g, ' '), 
                                away: away.replace(/\s+/g, ' ')
                            });
                        }
                    }
                });
                return data;
            """)
            
            if not jogos_raw:
                log.debug(f"Nenhum jogo agendado encontrado para {cfg_name}.")
                if pbar: pbar.update(1)
                continue
                
            log.debug(f"Encontrados {len(jogos_raw)} jogos agendados no calendário.")
            
            jogos_adicionados_liga = 0
            for j in jogos_raw:
                match_id = j['Match_ID']
                raw_time = j.get("timeStr", "").strip()
                
                if not raw_time: continue
                
                # Parsear data (Formato Flashscore agendado: "20.05. 14:00" ou "20.05. 2026")
                try:
                    parts = raw_time.split(" ")
                    date_part = parts[0].rstrip(".") # "20.05"
                    hr = parts[1] if len(parts) > 1 else "" # "14:00"
                    
                    day_str, month_str = date_part.split(".")
                    day = int(day_str)
                    month = int(month_str)
                    
                    # Tratar rotação do ano para agendamentos futuros
                    now = datetime.now()
                    year = now.year
                    if month < now.month and now.month - month > 6:
                        year += 1
                    elif month > now.month and month - now.month > 6:
                        year -= 1
                        
                    dt_str = f"{day:02d}/{month:02d}/{year}"
                except Exception as e:
                    log.warning(f"Erro ao parsear data '{raw_time}': {e}")
                    continue
                
                # Filtrar se o jogo está nas datas de interesse
                if target_dates:
                    if dt_str not in target_dates:
                        continue
                else:
                    try:
                        match_dt = datetime.strptime(dt_str, "%d/%m/%Y")
                        dias_diff = (match_dt - datetime.now()).days
                        if dias_diff > dias_futuros:
                            # Pular se estiver muito distante no futuro
                            continue
                    except:
                        pass
                
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
                    "home_score": None,
                    "away_score": None,
                    "home_score_ht": None,
                    "away_score_ht": None,
                    "home_goals_minutes": "",
                    "away_goals_minutes": ""
                }
                
                # Grava no banco na tabela matches
                save_dict("matches", match_data)
                
                # Inicializa na tabela stats como 0 (Não coletado) para o pipeline coletar os scouts pós-jogo
                save_dict("stats", {"match_id": match_id, "stats_collected": 0})
                
                jogos_adicionados_liga += 1
                total_adicionados += 1
                
            if jogos_adicionados_liga > 0:
                log.info(f"✅ {jogos_adicionados_liga} novos jogos agendados importados para {cfg_name}.")
            
            if pbar:
                pbar.update(1)
                
    finally:
        driver.quit()
        
    log.info(f"🎉 Importação de Calendário concluída! {total_adicionados} jogos agendados inseridos no banco.")
    return total_adicionados

if __name__ == "__main__":
    coletar_fixtures()
