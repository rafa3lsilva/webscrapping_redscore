import sys
import logging
from pathlib import Path
from tqdm import tqdm

# Adiciona o diretório raiz ao path para poder importar os módulos
sys.path.append(str(Path(__file__).parent))

from collector.flashscore.get_matches import coletar_matches
from collector.flashscore.get_odds import coletar_odds
from collector.flashscore.get_stats import coletar_stats
from database.db_manager import export_joined_csv, get_connection
from config.leagues import LIGAS_FLASHSCORE

# Configura o logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def formata_tempo(segundos):
    horas = segundos // 3600
    minutos = (segundos % 3600) // 60
    if horas > 0:
        return f"{int(horas)}h {int(minutos)}m"
    return f"{int(minutos)} min"

def run_pipeline():
    # -------------------------------------------------------------------------
    # 📊 CÁLCULO DE ESTIMATIVAS INICIAIS (DIAGNÓSTICO PRÉ-COLETA)
    # -------------------------------------------------------------------------
    total_seasons = 0
    for cfg_name, cfg in LIGAS_FLASHSCORE.items():
        if cfg.get("ativo", True):
            total_seasons += len(cfg.get("temporadas", []))
            
    with get_connection() as conn:
        # Jogos sem odds coletadas
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM odds)
        """)
        pending_odds = cursor.fetchone()[0]
        
        # Jogos sem stats coletadas
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM stats WHERE stats_collected = 1 OR stats_collected = -1)
        """)
        pending_stats = cursor.fetchone()[0]

    # Estimativa de tempo em segundos
    est_matches_sec = total_seasons * 15  # Média de 15s para carregar e expandir cada temporada
    est_odds_sec = pending_odds * 1.3     # Média de 1.3s por requisição GraphQL
    est_stats_sec = pending_stats * 2.3   # Média de 2.3s por requisição feed HTML
    tempo_total_sec = est_matches_sec + est_odds_sec + est_stats_sec

    # Dashboard Inicial
    log.info("=" * 60)
    log.info("      📈 PAINEL DE ESTIMATIVAS - REDSCORE PIPELINE V3")
    log.info("=" * 60)
    log.info(f" -> Ligas Ativas no Config:  20 ligas")
    log.info(f" -> Temporadas a verificar:  {total_seasons} temporadas (Est: {formata_tempo(est_matches_sec)})")
    log.info(f" -> Jogos pendentes de Odds: {pending_odds} jogos (Est: {formata_tempo(est_odds_sec)})")
    log.info(f" -> Jogos pendentes de Stats:{pending_stats} jogos (Est: {formata_tempo(est_stats_sec)})")
    log.info("-" * 60)
    log.info(f" >> TEMPO TOTAL ESTIMADO DO PIPELINE: {formata_tempo(tempo_total_sec)}")
    log.info("=" * 60)
    
    # Inicializa a barra de progresso global
    total_steps = total_seasons + pending_odds + pending_stats
    pbar = tqdm(total=total_steps, desc="Progresso Geral Pipeline", unit="step", position=0, leave=True)
    
    # -------------------------------------------------------------------------
    # 🟢 ETAPA 1: Base (Matches)
    # -------------------------------------------------------------------------
    log.info("--- [ETAPA 1] Extração de Partidas e Placares ---")
    try:
        coletar_matches(pbar=pbar)
    except Exception as e:
        log.error(f"Erro Crítico na Etapa 1: {e}")
        pbar.close()
        return
        
    # -------------------------------------------------------------------------
    # 🟢 ETAPA 2: Odds (GraphQL)
    # -------------------------------------------------------------------------
    # Recalcula as pendências reais de Odds caso novas partidas tenham sido inseridas na Etapa 1
    with get_connection() as conn:
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM odds)
        """)
        new_pending_odds = cursor.fetchone()[0]
        
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM stats WHERE stats_collected = 1 OR stats_collected = -1)
        """)
        new_pending_stats = cursor.fetchone()[0]
        
    # Ajusta dinamicamente o total do progresso geral
    pbar.total = pbar.n + new_pending_odds + new_pending_stats
    pbar.refresh()
    
    log.info("--- [ETAPA 2] Extração de Odds (Match, HT, Over/Under, BTTS) ---")
    try:
        coletar_odds(pbar=pbar)
    except Exception as e:
        log.error(f"Erro na Etapa 2: {e}")
        
    # -------------------------------------------------------------------------
    # 🟢 ETAPA 3: Estatísticas (Feed de Dados)
    # -------------------------------------------------------------------------
    # Recalcula as pendências finais de Stats
    with get_connection() as conn:
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matches 
            WHERE match_id NOT IN (SELECT match_id FROM stats WHERE stats_collected = 1 OR stats_collected = -1)
        """)
        final_pending_stats = cursor.fetchone()[0]
        
    pbar.total = pbar.n + final_pending_stats
    pbar.refresh()
    
    log.info("--- [ETAPA 3] Extração de Estatísticas Avançadas (xG, Corners) ---")
    try:
        coletar_stats(pbar=pbar)
    except Exception as e:
        log.error(f"Erro na Etapa 3: {e}")
        
    # Fecha a barra global com sucesso
    pbar.close()
    
    # -------------------------------------------------------------------------
    # 🟢 FINALIZAÇÃO
    # -------------------------------------------------------------------------
    log.info("--- [FINALIZAÇÃO] Exportando CSV Unificado ---")
    export_joined_csv()
    
    log.info("=" * 60)
    log.info("Pipeline Finalizado com Sucesso!")
    log.info("=" * 60)

if __name__ == "__main__":
    run_pipeline()
