import sys
import time
import re
import requests
import logging
from pathlib import Path
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database.db_manager import save_dict, get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.flashscore.com.br"
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-fsign': 'SW9D1eZo',
}

STAT_ID_MAP = {
    '432': 'xG', '499': 'xGOT', '503': 'xA', '501': 'xGOT_Faced',
    '511': 'Goals_Prevented', '12': 'Possession', '342': 'Passes_Pct',
    '517': 'Long_Passes_Pct', '467': 'Passes_Final_Third_Pct', '521': 'Through_Passes',
    '433': 'Crosses_Pct', '34': 'Total_Shots', '13': 'Shots_On_Target',
    '14': 'Shots_Off_Target', '158': 'Blocked_Shots', '461': 'Shots_Inside_Box',
    '463': 'Shots_Outside_Box', '459': 'Big_Chances', '457': 'Hit_Woodwork',
    '471': 'Touches_Box', '16': 'Corners', '15': 'Free_Kicks', '18': 'Throw_Ins',
    '21': 'Fouls', '17': 'Offsides', '19': 'Goalkeeper_Saves', '475': 'Tackles_Pct',
    '513': 'Duels_Won', '479': 'Clearances', '434': 'Interceptions',
    '507': 'Errors_Shot', '509': 'Errors_Goal',
}

def parse_value(raw: str):
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
            se = fields['SE'].lower()
            if '1' in se or '1º' in se:
                current_period = 'HT'
            elif '2' in se or '2º' in se:
                current_period = '2T'
            else:
                current_period = 'FT'
                
        if 'SD' in fields and 'SG' in fields:
            stat_id = fields['SD']
            col_name = STAT_ID_MAP.get(stat_id)
            if not col_name: continue
            
            key = f"{stat_id}_{current_period}"
            if key in seen: continue
            seen.add(key)
            
            result[f'{col_name}_Home_{current_period}'] = parse_value(fields.get('SH', ''))
            result[f'{col_name}_Away_{current_period}'] = parse_value(fields.get('SI', ''))
            
    return result

def fetch_stats_for_match(match_id: str, session: requests.Session) -> dict:
    url_stats = f'{BASE_URL}/x/feed/df_st_1_{match_id}'
    try:
        r = session.get(url_stats, headers={**HEADERS, 'referer': f'{BASE_URL}/jogo/{match_id}/'}, timeout=12)
        if r.status_code == 200 and len(r.text) > 5:
            stats_data = parse_stats_feed(r.text)
            stats_data["match_id"] = match_id
            stats_data["stats_collected"] = 1
            return stats_data
    except Exception as e:
        log.warning(f"Erro ao buscar stats para {match_id}: {e}")
        return None # Erro técnico, tenta de novo na próxima
        
    return {"match_id": match_id, "stats_collected": -1} # Confirmado sem stats

def coletar_stats():
    session = requests.Session()
    
    with get_connection() as conn:
        # Get matches that don't have stats collected yet
        cursor = conn.execute("""
            SELECT match_id FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM stats WHERE stats_collected = 1 OR stats_collected = -1)
        """)
        pending_ids = [row[0] for row in cursor.fetchall()]
        
    log.info(f"Total de jogos sem estatísticas avançadas: {len(pending_ids)}")
    
    for match_id in tqdm(pending_ids, desc="Coletando Stats"):
        stats_data = fetch_stats_for_match(match_id, session)
        if stats_data:
            save_dict("stats", stats_data)
        
        # Se stats_data for None, não salvamos nada (pula e tenta na próxima vez)
        time.sleep(0.3)

if __name__ == "__main__":
    coletar_stats()
