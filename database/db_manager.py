import sqlite3
import pandas as pd
import logging
from pathlib import Path

# Configure DB path
DB_DIR = Path(__file__).parent.parent / "data"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_NAME = DB_DIR / "flashscore_v3.db"
CSV_NAME = DB_DIR / "flashscore_v3.csv"

class SQLiteConnectionWrapper:
    def __init__(self, conn):
        self.conn = conn
    def __enter__(self):
        self.conn.__enter__()
        return self.conn
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            return self.conn.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self.conn.close()
    def __getattr__(self, name):
        return getattr(self.conn, name)

def get_connection():
    return SQLiteConnectionWrapper(sqlite3.connect(DB_NAME, timeout=60))

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
            over_ft_1_5 REAL, under_ft_1_5 REAL,
            over_ft_2_5 REAL, under_ft_2_5 REAL,
            over_ft_3_5 REAL, under_ft_3_5 REAL,
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
            # Lista Oficial StatsGreen para ML
            stats_green_allowed = {
                'match_id', 'country', 'league_full_name', 'league_code', 'season', 'round', 'date', 'time',
                'home_team', 'away_team', 'home_score', 'away_score', 'home_score_ht', 'away_score_ht',
                'home_goals_minutes', 'away_goals_minutes',
                'odd_h_ft', 'odd_d_ft', 'odd_a_ft',
                'odd_h_ht', 'odd_d_ht', 'odd_a_ht',
                'over_ft_1_5', 'under_ft_1_5', 'over_ft_2_5', 'under_ft_2_5', 'over_ft_3_5', 'under_ft_3_5',
                'btts_yes', 'btts_no', 'dc_1x', 'dc_12', 'dc_x2',
                'xG_Home_FT', 'xG_Away_FT', 'xGOT_Home_FT', 'xGOT_Away_FT',
                'Total_Shots_Home_FT', 'Total_Shots_Away_FT', 'Shots_On_Target_Home_FT', 'Shots_On_Target_Away_FT',
                'Big_Chances_Home_FT', 'Big_Chances_Away_FT', 'Corners_Home_FT', 'Corners_Away_FT',
                'Shots_Inside_Box_Home_FT', 'Shots_Inside_Box_Away_FT',
                'xG_Home_HT', 'xG_Away_HT', 'Shots_On_Target_Home_HT', 'Shots_On_Target_Away_HT',
                'Corners_Home_HT', 'Corners_Away_HT', 'stats_collected'
            }

            def get_cols(table, prefix):
                cursor = conn.execute(f"PRAGMA table_info({table})")
                cols = []
                for row in cursor.fetchall():
                    col_name = row[1]
                    # Filtro StatsGreen
                    if col_name not in stats_green_allowed: continue
                    if col_name == 'match_id' and prefix != 'm': continue
                    cols.append(f'{prefix}."{col_name}"')
                return cols
            
            m_cols = get_cols("matches", "m")
            o_cols = get_cols("odds", "o")
            s_cols = get_cols("stats", "s")
            
            # Garantir que stats_collected fique no final
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
            
            # Traduzir colunas misturando Português com termos padrão (home, away, draw, btts, dc)
            traducao = {
                'match_id': 'id_jogo', 'country': 'pais', 'league_full_name': 'liga', 
                'league_code': 'div', 'season': 'temporada', 'round': 'rodada', 
                'date': 'data', 'time': 'hora', 'home_team': 'home', 'away_team': 'away',
                'home_score': 'h_gols_ft', 'away_score': 'a_gols_ft',
                'home_score_ht': 'h_gols_ht', 'away_score_ht': 'a_gols_ht',
                'home_goals_minutes': 'h_min_gols', 'away_goals_minutes': 'a_min_gols',
                'odd_h_ft': 'odd_h_ft', 'odd_d_ft': 'odd_d_ft', 'odd_a_ft': 'odd_a_ft',
                'odd_h_ht': 'odd_h_ht', 'odd_d_ht': 'odd_d_ht', 'odd_a_ht': 'odd_a_ht',
                'over_ft_1.5': 'over_1.5_ft', 'under_ft_1.5': 'under_1.5_ft',
                'over_ft_2.5': 'over_2.5_ft', 'under_ft_2.5': 'under_2.5_ft',
                'over_ft_3.5': 'over_3.5_ft', 'under_ft_3.5': 'under_3.5_ft',
                'btts_yes': 'btts_yes', 'btts_no': 'btts_no',
                'dc_1x': 'dc_1x', 'dc_12': 'dc_12', 'dc_x2': 'dc_x2',
                'xG_Home_FT': 'xg_h_ft', 'xG_Away_FT': 'xg_a_ft',
                'xGOT_Home_FT': 'xgot_h_ft', 'xGOT_Away_FT': 'xgot_a_ft',
                'Total_Shots_Home_FT': 't_chutes_h_ft', 'Total_Shots_Away_FT': 't_chutes_a_ft',
                'Shots_On_Target_Home_FT': 'chutes_no_gol_home_ft', 'Shots_On_Target_Away_FT': 'chutes_no_gol_away_ft',
                'Big_Chances_Home_FT': 'grandes_chances_home_ft', 'Big_Chances_Away_FT': 'grandes_chances_away_ft',
                'Corners_Home_FT': 'escanteios_home_ft', 'Corners_Away_FT': 'escanteios_away_ft',
                'Shots_Inside_Box_Home_FT': 'chutes_na_area_home_ft', 'Shots_Inside_Box_Away_FT': 'chutes_na_area_away_ft',
                'xG_Home_HT': 'xg_h_ht', 'xG_Away_HT': 'xg_a_ht',
                'Shots_On_Target_Home_HT': 'chutes_no_gol_home_ht', 'Shots_On_Target_Away_HT': 'chutes_no_gol_away_ht',
                'Corners_Home_HT': 'escanteios_home_ht', 'Corners_Away_HT': 'escanteios_away_ht',
                'stats_collected': 'status_coleta'
            }
            df.rename(columns=traducao, inplace=True)
            
            df.to_csv(CSV_NAME, index=False)
            logging.info(f"✅ CSV Exportado com sucesso: {CSV_NAME}")
    except Exception as e:
        logging.error(f"Erro ao exportar CSV: {e}")
