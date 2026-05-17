import sqlite3
import shutil
import logging
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from database.db_manager import init_db, get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("migrate")

DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "flashscore_v3.db"
BACKUP_PATH = DB_DIR / "flashscore_v3_backup.db"

# List of allowed columns for Odds
ODDS_ALLOWED = [
    'match_id', 'odd_h_ft', 'odd_d_ft', 'odd_a_ft',
    'odd_h_ht', 'odd_d_ht', 'odd_a_ht',
    'over_ft_1_5', 'under_ft_1_5',
    'over_ft_2_5', 'under_ft_2_5',
    'over_ft_3_5', 'under_ft_3_5',
    'btts_yes', 'btts_no',
    'dc_1x', 'dc_12', 'dc_x2'
]

# List of allowed columns for Stats (StatsGreen)
STATS_ALLOWED = [
    'match_id', 'stats_collected',
    'xG_Home_FT', 'xG_Away_FT',
    'xGOT_Home_FT', 'xGOT_Away_FT',
    'Total_Shots_Home_FT', 'Total_Shots_Away_FT',
    'Shots_On_Target_Home_FT', 'Shots_On_Target_Away_FT',
    'Big_Chances_Home_FT', 'Big_Chances_Away_FT',
    'Corners_Home_FT', 'Corners_Away_FT',
    'Shots_Inside_Box_Home_FT', 'Shots_Inside_Box_Away_FT',
    'xG_Home_HT', 'xG_Away_HT',
    'Shots_On_Target_Home_HT', 'Shots_On_Target_Away_HT',
    'Corners_Home_HT', 'Corners_Away_HT'
]

def migrate():
    if not DB_PATH.exists():
        log.error("Banco de dados original não encontrado em %s", DB_PATH)
        return

    # 1. Criar backup
    log.info("Passo 1: Criando backup do banco de dados antigo...")
    shutil.copy2(DB_PATH, BACKUP_PATH)
    log.info("Backup criado com sucesso em: %s", BACKUP_PATH)

    # Conectar ao backup para ler os dados
    conn_bkp = sqlite3.connect(BACKUP_PATH)
    
    # 2. Deletar o DB original para recriá-lo limpo
    log.info("Passo 2: Removendo o banco de dados antigo e recriando com esquema limpo...")
    DB_PATH.unlink()
    init_db() # Recria matches, odds (limpa) e stats (inicial)
    log.info("Tabelas limpas inicializadas com sucesso.")

    # Conectar ao novo DB
    conn_new = get_connection()

    try:
        # 3. Migrar 'matches' (esquema idêntico, copia tudo)
        log.info("Passo 3: Migrando tabela 'matches'...")
        cursor_bkp = conn_bkp.execute("SELECT * FROM matches")
        cols = [d[0] for d in cursor_bkp.description]
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join([f'"{c}"' for c in cols])
        insert_sql = f"INSERT INTO matches ({col_names}) VALUES ({placeholders})"
        
        rows = cursor_bkp.fetchall()
        conn_new.executemany(insert_sql, rows)
        conn_new.commit()
        log.info("Tabela 'matches' migrada: %d registros.", len(rows))

        # 4. Migrar 'odds' (filtrando apenas colunas permitidas)
        log.info("Passo 4: Migrando tabela 'odds' (filtrando colunas obsoleto)...")
        # Descobrir quais colunas permitidas de fato existem na tabela antiga para evitar erros
        cursor_bkp_odds = conn_bkp.execute("PRAGMA table_info(odds)")
        existing_odds_cols = {row[1] for row in cursor_bkp_odds.fetchall()}
        odds_to_select = [c for c in ODDS_ALLOWED if c in existing_odds_cols]
        
        odds_query = f"SELECT {', '.join([f'\"{c}\"' for c in odds_to_select])} FROM odds"
        cursor_bkp_odds_data = conn_bkp.execute(odds_query)
        
        odds_rows = cursor_bkp_odds_data.fetchall()
        if odds_rows:
            odds_placeholders = ", ".join(["?"] * len(odds_to_select))
            odds_col_names = ", ".join([f'"{c}"' for c in odds_to_select])
            odds_insert_sql = f"INSERT INTO odds ({odds_col_names}) VALUES ({odds_placeholders})"
            conn_new.executemany(odds_insert_sql, odds_rows)
            conn_new.commit()
        log.info("Tabela 'odds' migrada: %d registros.", len(odds_rows))

        # 5. Migrar 'stats' (filtrando colunas StatsGreen e criando dinamicamente apenas as necessárias)
        log.info("Passo 5: Migrando tabela 'stats' (filtrando StatsGreen)...")
        cursor_bkp_stats = conn_bkp.execute("PRAGMA table_info(stats)")
        existing_stats_cols = {row[1] for row in cursor_bkp_stats.fetchall()}
        stats_to_select = [c for c in STATS_ALLOWED if c in existing_stats_cols]

        # Criar as colunas necessárias na nova tabela stats antes de inserir
        for col in stats_to_select:
            if col not in ('match_id', 'stats_collected', 'created_at'):
                conn_new.execute(f'ALTER TABLE stats ADD COLUMN "{col}" REAL')
        
        stats_query = f"SELECT {', '.join([f'\"{c}\"' for c in stats_to_select])} FROM stats"
        cursor_bkp_stats_data = conn_bkp.execute(stats_query)
        stats_rows = cursor_bkp_stats_data.fetchall()
        
        if stats_rows:
            stats_placeholders = ", ".join(["?"] * len(stats_to_select))
            stats_col_names = ", ".join([f'"{c}"' for c in stats_to_select])
            stats_insert_sql = f"INSERT INTO stats ({stats_col_names}) VALUES ({stats_placeholders})"
            conn_new.executemany(stats_insert_sql, stats_rows)
            conn_new.commit()
        log.info("Tabela 'stats' migrada: %d registros.", len(stats_rows))

        # 6. Rodar VACUUM para otimizar espaço em disco
        log.info("Passo 6: Compactando banco de dados (VACUUM)...")
        conn_new.execute("VACUUM")
        log.info("Banco de dados compactado com sucesso.")

    except Exception as e:
        log.error("Erro durante a migração: %s", e)
        conn_new.rollback()
        # Restaurar backup se der ruim
        log.info("Restaurando backup devido a erro...")
        shutil.copy2(BACKUP_PATH, DB_PATH)
    finally:
        conn_bkp.close()
        conn_new.close()

    # Mostrar comparativo de tamanhos
    sz_old = BACKUP_PATH.stat().st_size / (1024 * 1024)
    sz_new = DB_PATH.stat().st_size / (1024 * 1024)
    log.info("============================================================")
    log.info("✅ MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
    log.info("Tamanho do banco anterior: %.2f MB", sz_old)
    log.info("Tamanho do novo banco limpo: %.2f MB (Redução de %.1f%%)", sz_new, (1 - sz_new/sz_old)*100)
    log.info("============================================================")

if __name__ == "__main__":
    migrate()
