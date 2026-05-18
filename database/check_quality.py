import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "flashscore_v3.db"

def check_quality():
    if not DB_PATH.exists():
        print(f"Erro: Banco de dados não encontrado em {DB_PATH}")
        return
        
    conn = sqlite3.connect(DB_PATH)
    
    # Obter colunas reais do banco para evitar KeyError
    cursor = conn.execute("PRAGMA table_info(odds)")
    odds_cols = [r[1] for r in cursor.fetchall()]
    
    cursor = conn.execute("PRAGMA table_info(stats)")
    stats_cols = [r[1] for r in cursor.fetchall()]
    
    xg_col_exists = "xG_Home_FT" in stats_cols
    odd_col_exists = "odd_h_ft" in odds_cols
    
    select_fields = "m.league_full_name, m.season, COUNT(m.match_id) as total_jogos"
    if odd_col_exists:
        select_fields += ", SUM(CASE WHEN o.odd_h_ft IS NOT NULL THEN 1 ELSE 0 END) as com_odds"
    else:
        select_fields += ", 0 as com_odds"
        
    if xg_col_exists:
        select_fields += ", SUM(CASE WHEN s.xG_Home_FT IS NOT NULL THEN 1 ELSE 0 END) as com_xg"
    else:
        select_fields += ", 0 as com_xg"
        
    query = f"""
    SELECT {select_fields}
    FROM matches m
    LEFT JOIN odds o ON m.match_id = o.match_id
    LEFT JOIN stats s ON m.match_id = s.match_id
    GROUP BY m.league_full_name, m.season
    ORDER BY m.league_full_name, m.season DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    df['odds_cobertura_%'] = (df['com_odds'] / df['total_jogos'] * 100).round(1)
    df['xg_cobertura_%'] = (df['com_xg'] / df['total_jogos'] * 100).round(1)
    
    print(df.to_markdown(index=False))

if __name__ == "__main__":
    check_quality()
