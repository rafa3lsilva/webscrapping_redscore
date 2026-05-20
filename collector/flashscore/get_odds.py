import sys
import time
import requests
import logging
import concurrent.futures
import threading
from pathlib import Path
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database.db_manager import save_dict, get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def parse_value(v):
    try: return float(v)
    except: return None

def fetch_odds_for_match(match_id: str, session: requests.Session) -> dict:
    res = {"match_id": match_id}
    url = "https://global.ds.lsapp.eu/odds/pq_graphql"
    base_params = {"eventId": match_id, "projectId": "401", "geoIpCode": "BR", "geoIpSubdivisionCode": "BR"}
    headers = {"user-agent": "Mozilla/5.0"}

    # We use 'oce' hash which provides full bettingScope
    try:
        r = session.get(url, params={**base_params, "_hash": "oce"}, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json().get("data", {}).get("findOddsByEventId", {}).get("odds", [])
            for market in data:
                bt = market.get("bettingType")
                scope = market.get("bettingScope", "FULL_TIME")
                odds = market.get("odds", [])
                
                # Match Odds
                if bt == "HOME_DRAW_AWAY" and scope == "FULL_TIME" and len(odds) >= 3:
                    if not res.get("odd_h_ft"):
                        non_draw = []
                        d_val = None
                        for o in odds:
                            val = parse_value(str(o.get("opening") or o.get("value")))
                            if o.get("eventParticipantId") is None:
                                d_val = val
                            else:
                                non_draw.append(val)
                        if len(non_draw) >= 2:
                            res["odd_h_ft"] = non_draw[0]
                            res["odd_a_ft"] = non_draw[1]
                        res["odd_d_ft"] = d_val
                elif bt == "HOME_DRAW_AWAY" and scope == "FIRST_HALF" and len(odds) >= 3:
                    if not res.get("odd_h_ht"):
                        non_draw = []
                        d_val = None
                        for o in odds:
                            val = parse_value(str(o.get("opening") or o.get("value")))
                            if o.get("eventParticipantId") is None:
                                d_val = val
                            else:
                                non_draw.append(val)
                        if len(non_draw) >= 2:
                            res["odd_h_ht"] = non_draw[0]
                            res["odd_a_ht"] = non_draw[1]
                        res["odd_d_ht"] = d_val
                
                # BTTS
                elif bt == "BOTH_TEAMS_TO_SCORE" and scope == "FULL_TIME":
                    if not res.get("btts_yes") and len(odds) >= 2:
                        for o in odds:
                            val = parse_value(str(o.get("opening") or o.get("value")))
                            if o.get("bothTeamsToScore") is True:
                                res["btts_yes"] = val
                            elif o.get("bothTeamsToScore") is False:
                                res["btts_no"] = val
                
                # Double Chance
                elif bt == "DOUBLE_CHANCE" and scope == "FULL_TIME":
                    if not res.get("dc_1x") and len(odds) >= 3:
                        non_draw = []
                        dc_12_val = None
                        for o in odds:
                            val = parse_value(str(o.get("opening") or o.get("value")))
                            if o.get("eventParticipantId") is None:
                                dc_12_val = val
                            else:
                                non_draw.append(val)
                        if len(non_draw) >= 2:
                            res["dc_1x"] = non_draw[0]
                            res["dc_x2"] = non_draw[1]
                        res["dc_12"] = dc_12_val
                
                # Over/Under
                elif bt == "OVER_UNDER":
                    grouped = {}
                    prefix = "ft" if scope == "FULL_TIME" else "ht"
                    for o in odds:
                        h_val = str(o.get("handicap", {}).get("value", ""))
                        sel = o.get("selection")
                        val = parse_value(str(o.get("opening") or o.get("value")))
                        if h_val and h_val.endswith(".5") and sel and val is not None:
                            try:
                                h_num = float(h_val)
                                # Limites StatsGreen: FT apenas 1.5, 2.5 e 3.5. Ignorar HT O/U.
                                if scope == "FULL_TIME":
                                    if h_num not in [1.5, 2.5, 3.5]: continue
                                else:
                                    continue # Remover todo o mercado HT de O/U
                                
                                h_val_db = h_val.replace(".", "_") # SQLite column names format
                                if h_val_db not in grouped: grouped[h_val_db] = {}
                                grouped[h_val_db][sel] = val
                            except: continue
                    
                    for h_val_db, sels in grouped.items():
                        if "OVER" in sels and "UNDER" in sels:
                            res[f"over_{prefix}_{h_val_db}"] = sels["OVER"]
                            res[f"under_{prefix}_{h_val_db}"] = sels["UNDER"]
    except Exception as e:
        log.error(f"Error fetching odds for {match_id}: {e}")
        return None # Indica erro de rede/servidor, tenta de novo na próxima
        
    return res

def coletar_odds(pbar=None, max_workers=4):
    with get_connection() as conn:
        # Get matches that don't have odds yet
        cursor = conn.execute("""
            SELECT match_id FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM odds)
        """)
        pending_ids = [row[0] for row in cursor.fetchall()]
        
    log.info(f"Total de jogos sem odds: {len(pending_ids)}")
    if not pending_ids:
        return
        
    pbar_lock = threading.Lock()
    thread_local = threading.local()
    
    def worker(match_id):
        # Garante session persistente por thread
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        session = thread_local.session
        
        odds_data = fetch_odds_for_match(match_id, session)
        
        if odds_data is None:
            # Erro técnico, não salva para tentar de novo depois
            return
            
        if len(odds_data) > 1: # Sucesso, tem odds
            save_dict("odds", odds_data)
        else:
            # Confirmado sem odds
            save_dict("odds", {"match_id": match_id})
            
        if pbar:
            with pbar_lock:
                pbar.update(1)
                pbar.set_description(f"Odds: {match_id}")
                
        # Delay de segurança por thread para suavizar o ritmo de requisições
        time.sleep(0.4)
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Consome o gerador para forçar a execução completa de todas as threads
        list(executor.map(worker, pending_ids))

if __name__ == "__main__":
    coletar_odds()
