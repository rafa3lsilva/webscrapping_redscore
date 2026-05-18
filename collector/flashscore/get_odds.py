import sys
import time
import requests
import logging
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
                        res["odd_h_ft"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["odd_d_ft"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                        res["odd_a_ft"] = parse_value(str(odds[2].get("opening") or odds[2].get("value")))
                elif bt == "HOME_DRAW_AWAY" and scope == "FIRST_HALF" and len(odds) >= 3:
                    if not res.get("odd_h_ht"):
                        res["odd_h_ht"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["odd_d_ht"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                        res["odd_a_ht"] = parse_value(str(odds[2].get("opening") or odds[2].get("value")))
                
                # BTTS
                elif bt == "BOTH_TEAMS_TO_SCORE" and scope == "FULL_TIME":
                    if not res.get("btts_yes") and len(odds) >= 2:
                        res["btts_yes"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["btts_no"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                
                # Double Chance
                elif bt == "DOUBLE_CHANCE" and scope == "FULL_TIME":
                    if not res.get("dc_1x") and len(odds) >= 3:
                        res["dc_1x"] = parse_value(str(odds[0].get("opening") or odds[0].get("value")))
                        res["dc_12"] = parse_value(str(odds[1].get("opening") or odds[1].get("value")))
                        res["dc_x2"] = parse_value(str(odds[2].get("opening") or odds[2].get("value")))
                
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

def coletar_odds(pbar=None):
    session = requests.Session()
    
    with get_connection() as conn:
        # Get matches that don't have odds yet
        cursor = conn.execute("""
            SELECT match_id FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM odds)
        """)
        pending_ids = [row[0] for row in cursor.fetchall()]
        
    log.info(f"Total de jogos sem odds: {len(pending_ids)}")
    
    loop_iter = tqdm(pending_ids, desc="Coletando Odds") if not pbar else pending_ids
    for match_id in loop_iter:
        odds_data = fetch_odds_for_match(match_id, session)
        
        if odds_data is None:
            # Erro técnico (rede/timeout), não salva para tentar novamente depois
            continue
            
        if len(odds_data) > 1: # Sucesso, tem odds
            save_dict("odds", odds_data)
        else:
            # Visitado com sucesso, mas o servidor confirmou que não tem odds
            save_dict("odds", {"match_id": match_id})
            
        if pbar:
            pbar.update(1)
            pbar.set_description(f"Odds: {match_id}")
            
        time.sleep(0.2)

if __name__ == "__main__":
    coletar_odds()
