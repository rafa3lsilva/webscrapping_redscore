import sqlite3
import logging
from pathlib import Path

# Configuração de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path("data/flashscore_v3.db")

def limpar_erros():
    if not DB_PATH.exists():
        log.error(f"Banco de dados não encontrado em {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. Limpar Odds "fantasmas" (registros sem dados reais de aposta)
        # Isso remove jogos que foram marcados como visitados mas falharam por rede na Etapa 2
        cursor.execute("DELETE FROM odds WHERE odd_h_ft IS NULL AND btts_yes IS NULL")
        odds_removidas = cursor.rowcount

        # 2. Limpar Stats marcadas como inexistentes (-1)
        # Isso dá uma segunda chance para a Etapa 3 verificar se realmente não há xG/Stats
        cursor.execute("DELETE FROM stats WHERE stats_collected = -1")
        stats_removidas = cursor.rowcount

        conn.commit()
        conn.close()

        log.info("============================================================")
        log.info("SISTEMA DE LIMPEZA DE ERROS")
        log.info("============================================================")
        log.info(f"-> Odds resetadas (para re-coleta): {odds_removidas}")
        log.info(f"-> Stats resetadas (para re-coleta): {stats_removidas}")
        log.info("============================================================")
        log.info("Concluído! Agora você pode rodar o pipeline_runner.py")
        log.info("Ele focará apenas nesses jogos que haviam falhado.")

    except Exception as e:
        log.error(f"Erro ao limpar banco de dados: {e}")

if __name__ == "__main__":
    limpar_erros()
