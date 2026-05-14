import sys
import logging
from pathlib import Path

# Adiciona o diretório raiz ao path para poder importar os módulos
sys.path.append(str(Path(__file__).parent))

from collector.flashscore.get_matches import coletar_matches
from collector.flashscore.get_odds import coletar_odds
from collector.flashscore.get_stats import coletar_stats
from database.db_manager import export_joined_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def run_pipeline():
    log.info("=" * 60)
    log.info("Iniciando Pipeline RedScore V3 (3 Etapas)")
    log.info("=" * 60)
    
    # Etapa 1: Base (Matches)
    log.info("--- [ETAPA 1] Extração de Partidas e Placares ---")
    try:
        coletar_matches()
    except Exception as e:
        log.error(f"Erro Crítico na Etapa 1: {e}")
        return # Se falhar aqui, não tem sentido continuar as outras
    
    # Etapa 2: Odds (GraphQL)
    log.info("--- [ETAPA 2] Extração de Odds (Match, HT, Over/Under, BTTS) ---")
    try:
        coletar_odds()
    except Exception as e:
        log.error(f"Erro na Etapa 2: {e}")
        # Continua mesmo se der erro, pois podemos pegar as stats
        
    # Etapa 3: Estatísticas (Feed de Dados)
    log.info("--- [ETAPA 3] Extração de Estatísticas Avançadas (xG, Corners) ---")
    try:
        coletar_stats()
    except Exception as e:
        log.error(f"Erro na Etapa 3: {e}")
        
    # Finalização
    log.info("--- [FINALIZAÇÃO] Exportando CSV Unificado ---")
    export_joined_csv()
    
    log.info("=" * 60)
    log.info("Pipeline Finalizado com Sucesso!")
    log.info("=" * 60)

if __name__ == "__main__":
    run_pipeline()
