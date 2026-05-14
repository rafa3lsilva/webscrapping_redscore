import sqlite3
import pandas as pd
import logging
from pathlib import Path

# Configure DB path
DB_DIR = Path(__file__).parent.parent / "data"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_NAME = DB_DIR / "flashscore_v3.db"
CSV_NAME = DB_DIR / "flashscore_v3.csv"

def get_connection():
    return sqlite3.connect(DB_NAME, timeout=60)

def init_db():
    """Initializes the three main tables according to the architecture skill."""
    with get_connection() as conn:
        # Table 1: Matches
        conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            country TEXT,
            league_full_name TEXT,
            league_code TEXT,
            season INTEGER,
            round TEXT,
            date TEXT,
            time TEXT,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            home_score_ht INTEGER,
            away_score_ht INTEGER,
            home_goals_minutes TEXT,
            away_goals_minutes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        # Table 2: Odds
        conn.execute("""
        CREATE TABLE IF NOT EXISTS odds (
            match_id TEXT PRIMARY KEY,
            odd_h_ft REAL, odd_d_ft REAL, odd_a_ft REAL,
            odd_h_ht REAL, odd_d_ht REAL, odd_a_ht REAL,
            over_ft_0_5 REAL, under_ft_0_5 REAL,
            over_ft_1_5 REAL, under_ft_1_5 REAL,
            over_ft_2_5 REAL, under_ft_2_5 REAL,
            over_ft_3_5 REAL, under_ft_3_5 REAL,
            over_ft_4_5 REAL, under_ft_4_5 REAL,
            over_ht_0_5 REAL, under_ht_0_5 REAL,
            over_ht_1_5 REAL, under_ht_1_5 REAL,
            over_ht_2_5 REAL, under_ht_2_5 REAL,
            btts_yes REAL, btts_no REAL,
            dc_1x REAL, dc_12 REAL, dc_x2 REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # Table 3: Stats
        conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            match_id TEXT PRIMARY KEY,
            stats_collected INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()

def save_dict(table: str, data: dict):
    """Generic function to insert or update a dictionary into a specific table."""
    if not data: return
    
    with get_connection() as conn:
        # Dynamically add columns if they don't exist
        cursor = conn.execute(f"PRAGMA table_info({table})")
        existing_cols = {row[1] for row in cursor.fetchall()}
        
        for col in data.keys():
            if col not in existing_cols:
                col_type = "REAL" if isinstance(data[col], (int, float)) else "TEXT"
                try: 
                    conn.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {col_type}')
                except Exception as e:
                    logging.warning(f"Error adding column {col}: {e}")
        
        # Insert or Replace
        cols = list(data.keys())
        placeholders = ', '.join(['?'] * len(cols))
        col_names = ', '.join([f'"{c}"' for c in cols])
        sql = f'INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})'
        conn.execute(sql, [data[c] for c in cols])
        conn.commit()

def export_joined_csv():
    """Performs a full outer-like join to export the complete dataset."""
    try:
        with get_connection() as conn:
            # Dynamically get columns to avoid repeating match_id and include all dynamic stats
            def get_cols(table, prefix):
                cursor = conn.execute(f"PRAGMA table_info({table})")
                # exclude match_id and created_at from duplicate columns
                cols = []
                for row in cursor.fetchall():
                    col_name = row[1]
                    if col_name == 'match_id' and prefix != 'm': continue
                    if col_name == 'created_at': continue
                    cols.append(f'{prefix}."{col_name}"')
                return cols
            
            m_cols = get_cols("matches", "m")
            o_cols = get_cols("odds", "o")
            s_cols = get_cols("stats", "s")
            
            # Remover stats_collected da ordem normal e jogar pro final
            stats_collected_col = 's."stats_collected"'
            if stats_collected_col in s_cols:
                s_cols.remove(stats_collected_col)
                all_cols_list = m_cols + o_cols + s_cols + [stats_collected_col]
            else:
                all_cols_list = m_cols + o_cols + s_cols
                
            all_cols = ", ".join(all_cols_list)
            
            query = f"""
            SELECT {all_cols}
            FROM matches m
            LEFT JOIN odds o ON m.match_id = o.match_id
            LEFT JOIN stats s ON m.match_id = s.match_id
            """
            
            df = pd.read_sql(query, conn)
            # Reformat Odds names (ex: over_ft_2_5 -> over_ft_2.5)
            import re
            df.columns = [re.sub(r'_(\d+)_5$', r'_\1.5', c) for c in df.columns]
            
            df.to_csv(CSV_NAME, index=False)
            logging.info(f"✅ CSV Exportado com sucesso: {CSV_NAME}")
    except Exception as e:
        logging.error(f"Erro ao exportar CSV: {e}")
