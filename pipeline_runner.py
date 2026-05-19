import sys
import logging
import argparse
import requests
import re
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm

# Adiciona o diretório raiz ao path para poder importar os módulos
sys.path.append(str(Path(__file__).parent))

from collector.flashscore.get_matches import coletar_matches
from collector.flashscore.get_fixtures import coletar_fixtures
from collector.flashscore.get_odds import coletar_odds
from collector.flashscore.get_stats import coletar_stats
from database.db_manager import export_joined_csv, get_connection, save_dict
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

def exportar_jogos_do_dia(target_dates):
    """
    Exporta CSVs dedicados e separados para cada uma das datas especificadas,
    seguindo o padrão de nomenclatura estrito: Jogos_do_Dia_Flashscore_DD-MM-YYYY.csv
    dentro da pasta dedicada data/jogos_do_dia/
    """
    dest_dir = Path(__file__).parent / "data" / "jogos_do_dia"
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    for dt_str in target_dates:
        # Formatar a data para o nome do arquivo (ex: "20/05/2026" -> "20-05-2026")
        dt_filename = dt_str.replace("/", "-")
        csv_path = dest_dir / f"Jogos_do_Dia_Flashscore_{dt_filename}.csv"
        
        with get_connection() as conn:
            # Lista Oficial de colunas permitidas para manter o padrão StatsGreen
            stats_green_allowed = {
                'match_id', 'country', 'league_full_name', 'league_code', 'season', 'round', 'date', 'time',
                'home_team', 'away_team', 'home_score', 'away_score', 'home_score_ht', 'away_score_ht',
                'home_goals_minutes', 'away_goals_minutes',
                'odd_h_ft', 'odd_d_ft', 'odd_a_ft',
                'odd_h_ht', 'odd_d_ht', 'odd_a_ht',
                'over_ft_1_5', 'under_ft_1_5', 'over_ft_2_5', 'under_ft_2_5', 'over_ft_3_5', 'under_ft_3_5',
                'btts_yes', 'btts_no', 'dc_1x', 'dc_12', 'dc_x2'
            }

            def get_cols(table, prefix):
                cursor = conn.execute(f"PRAGMA table_info({table})")
                cols = []
                for row in cursor.fetchall():
                    col_name = row[1]
                    if col_name not in stats_green_allowed: continue
                    if col_name == 'match_id' and prefix != 'm': continue
                    cols.append(f'{prefix}."{col_name}"')
                return cols
            
            m_cols = get_cols("matches", "m")
            o_cols = get_cols("odds", "o")
            
            if not m_cols:
                continue
                
            all_cols = ", ".join(m_cols + o_cols)
            
            query = f"""
            SELECT {all_cols}
            FROM matches m
            LEFT JOIN odds o ON m.match_id = o.match_id
            WHERE m.date = ?
            """
            
            df = pd.read_sql(query, conn, params=(dt_str,))
            
        if df.empty:
            log.info(f"Nenhum jogo cadastrado no banco para a data: {dt_str}")
            continue
            
        # Reformatar nomes das Odds
        df.columns = [re.sub(r'_(\d+)_5$', r'_\1.5', c) for c in df.columns]
        
        # Tradução idêntica das colunas
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
            'dc_1x': 'dc_1x', 'dc_12': 'dc_12', 'dc_x2': 'dc_x2'
        }
        df.rename(columns=traducao, inplace=True)
        
        df.to_csv(csv_path, index=False)
        log.info(f"✅ CSV de Jogos do Dia exportado com sucesso: {csv_path}")

def atualizar_jogos_concluidos(target_dates):
    """
    Função super leve baseada em APIs para atualizar placares finais
    e de HT para jogos agendados que já finalizaram.
    """
    # Converter para tupla para a query SQL
    dates_tuple = tuple(target_dates)
    placeholders = ", ".join(["?"] * len(target_dates))
    
    # 1. Encontrar jogos pendentes (sem placar) nas datas de interesse
    with get_connection() as conn:
        cursor = conn.execute(
            f"SELECT match_id, country, league_full_name, league_code, season, round, date, time, home_team, away_team "
            f"FROM matches WHERE home_score IS NULL AND date IN ({placeholders})",
            dates_tuple
        )
        pending_matches = cursor.fetchall()
        
    if not pending_matches:
        log.info("Nenhum jogo pendente encontrado para atualizar placar nas datas alvo.")
        return 0
        
    log.info(f"Encontrados {len(pending_matches)} jogos pendentes para atualização de resultados pós-jogo.")
    
    session = requests.Session()
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'x-fsign': 'SW9D1eZo',
    }
    base_url = "https://www.flashscore.com.br"
    
    updated_count = 0
    
    for row in tqdm(pending_matches, desc="Atualizando ontem/hoje"):
        match_id, country, league_full_name, league_code, season, round_name, dt_str, hr, home_team, away_team = row
        
        url_dc = f"{base_url}/x/feed/dc_1_{match_id}"
        home_ft, away_ft = None, None
        
        try:
            r = session.get(url_dc, headers={**headers, 'referer': f'{base_url}/jogo/{match_id}/'}, timeout=5)
            if r.status_code == 200:
                fields = {}
                for pair in r.text.split('¬'):
                    if '÷' in pair:
                        k, v = pair.split('÷', 1)
                        fields[k] = v
                        
                def parse_value(v):
                    try: return int(v)
                    except: return None
                    
                if 'DE' in fields: home_ft = parse_value(fields['DE'])
                if 'DF' in fields: away_ft = parse_value(fields['DF'])
        except Exception as e:
            continue
            
        # Se ainda não tem placar definido, a partida pode não ter começado ou terminado ainda
        if home_ft is None or away_ft is None:
            continue
            
        # O jogo terminou! Vamos buscar o placar HT correto e minutos de gols
        home_ht, away_ht = None, None
        home_goals_minutes, away_goals_minutes = "", ""
        
        url_su = f"{base_url}/x/feed/df_su_1_{match_id}"
        try:
            r_su = session.get(url_su, headers={**headers, 'referer': f'{base_url}/jogo/{match_id}/'}, timeout=5)
            if r_su.status_code == 200:
                m_ht = re.search(r'AC÷1(?:º|\.) tempo.*?IG÷(\d+).*?IH÷(\d+)', r_su.text)
                if m_ht:
                    home_ht = int(m_ht.group(1))
                    away_ht = int(m_ht.group(2))
                
                home_mins, away_mins = [], []
                blocks = r_su.text.split('~III÷')
                for b in blocks:
                    if 'IK÷Gol' in b or 'IK÷Pênalti' in b:
                        m_min = re.search(r'IB÷([\d\+\']+)', b)
                        m_side = re.search(r'IA÷(\d)', b)
                        if m_min and m_side:
                            minute = m_min.group(1).replace("'", "")
                            if m_side.group(1) == '1': home_mins.append(minute)
                            else: away_mins.append(minute)
                            
                home_goals_minutes = f"[{', '.join(home_mins)}]"
                away_goals_minutes = f"[{', '.join(away_mins)}]"
        except:
            pass
            
        # Salvar partida atualizada no banco
        match_data = {
            "match_id": match_id,
            "country": country,
            "league_full_name": league_full_name,
            "league_code": league_code,
            "season": season,
            "round": round_name,
            "date": dt_str,
            "time": hr,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_ft,
            "away_score": away_ft,
            "home_score_ht": home_ht,
            "away_score_ht": away_ht,
            "home_goals_minutes": home_goals_minutes,
            "away_goals_minutes": away_goals_minutes
        }
        save_dict("matches", match_data)
        updated_count += 1
        
    log.info(f"✅ {updated_count} jogos finalizados tiveram seus placares atualizados com sucesso!")
    return updated_count

def run_pipeline(mode="results"):
    now_dt = datetime.now()
    yesterday = (now_dt - timedelta(days=1)).strftime("%d/%m/%Y")
    today = now_dt.strftime("%d/%m/%Y")
    tomorrow = (now_dt + timedelta(days=1)).strftime("%d/%m/%Y")

    # -------------------------------------------------------------------------
    # ⚡ MODO DAILY (COLETA AMANHÃ, ATUALIZA ONTEM)
    # -------------------------------------------------------------------------
    if mode == "daily":
        active_leagues_count = len([k for k, v in LIGAS_FLASHSCORE.items() if v.get("ativo", True)])
        
        log.info("=" * 60)
        log.info("      📅 PIPELINE DIÁRIO AUTOMÁTICO - REDSCORE V3")
        log.info("=" * 60)
        log.info(f" -> Ligas Ativas no Config:  {active_leagues_count} ligas")
        log.info(f" -> Data Ontem (Atualizar): {yesterday}")
        log.info(f" -> Data Hoje (Monitorar):   {today}")
        log.info(f" -> Data Amanhã (Coletar):   {tomorrow}")
        log.info("=" * 60)
        
        # 1. Coleta de Jogos Futuros (Amanhã e Hoje)
        log.info("--- [ETAPA 1/4] Coleta de Jogos Agendados (Hoje/Amanhã) ---")
        pbar_cal = tqdm(total=active_leagues_count, desc="Lendo Calendários", unit="liga")
        try:
            coletar_fixtures(target_dates=[today, tomorrow], pbar=pbar_cal)
        except Exception as e:
            log.error(f"Erro ao coletar calendário: {e}")
        pbar_cal.close()
        
        # 2. Coleta de Odds Pré-Jogo para os jogos agendados
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM matches 
                WHERE match_id NOT IN (SELECT match_id FROM odds)
            """)
            pending_odds = cursor.fetchone()[0]
            
        if pending_odds > 0:
            log.info(f"--- [ETAPA 2/4] Coleta de Odds Pré-Jogo ({pending_odds} pendentes) ---")
            pbar_odds = tqdm(total=pending_odds, desc="Coletando Odds", unit="jogo")
            try:
                coletar_odds(pbar=pbar_odds)
            except Exception as e:
                log.error(f"Erro ao coletar odds pré-jogo: {e}")
            pbar_odds.close()
        else:
            log.info("Nenhuma Odd Pré-Jogo pendente para coletar.")
            
        # 3. Atualização Leve de Resultados de Ontem (e de Hoje já terminados)
        log.info("--- [ETAPA 3/4] Atualizando Placares Ontem/Hoje ---")
        atualizar_jogos_concluidos(target_dates=[yesterday, today])
        
        # 4. Coleta de Scouts e xG dos jogos que acabaram de ser atualizados
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM matches 
                WHERE home_score IS NOT NULL 
                  AND match_id NOT IN (SELECT match_id FROM stats WHERE stats_collected = 1 OR stats_collected = -1)
            """)
            pending_stats = cursor.fetchone()[0]
            
        if pending_stats > 0:
            log.info(f"--- [ETAPA 4/4] Coleta de Scouts/xG pós-jogo ({pending_stats} pendentes) ---")
            pbar_stats = tqdm(total=pending_stats, desc="Coletando Stats", unit="jogo")
            try:
                coletar_stats(pbar=pbar_stats)
            except Exception as e:
                log.error(f"Erro ao coletar estatísticas pós-jogo: {e}")
            pbar_stats.close()
        else:
            log.info("Nenhum scout/xG pendente para coletar pós-jogo.")
            
        # 5. Exportações de finalização
        log.info("--- [FINALIZAÇÃO] Exportando CSV Unificado ---")
        export_joined_csv()
        
        log.info("--- [FINALIZAÇÃO] Exportando CSVs Dedicados de Jogos do Dia ---")
        exportar_jogos_do_dia([today, tomorrow])
        
        log.info("=" * 60)
        log.info("Pipeline Diário Completo Executado com Sucesso!")
        log.info("=" * 60)
        return

    # -------------------------------------------------------------------------
    # 📊 MODO UPCOMING / FIXTURES (PRÓXIMOS JOGOS GERAIS E ODDS PRÉ-JOGO)
    # -------------------------------------------------------------------------
    if mode == "fixtures":
        active_leagues_count = len([k for k, v in LIGAS_FLASHSCORE.items() if v.get("ativo", True)])
        
        log.info("=" * 60)
        log.info("      📈 PIPELINE DIÁRIO DE PRÓXIMOS JOGOS - REDSCORE V3")
        log.info("=" * 60)
        log.info(f" -> Ligas Ativas no Config:  {active_leagues_count} ligas")
        log.info(f" -> Objetivo: Mapear próximos jogos e extrair Odds Pré-Jogo")
        log.info("=" * 60)
        
        pbar = tqdm(total=active_leagues_count, desc="Progresso Geral Calendário", unit="liga")
        
        log.info("--- [ETAPA 1/2] Extração de Próximos Jogos (Calendário) ---")
        try:
            coletar_fixtures(pbar=pbar)
        except Exception as e:
            log.error(f"Erro Crítico na Etapa 1 do Calendário: {e}")
            pbar.close()
            return
            
        pbar.close()
        
        # Calcular pendências de odds após importar os novos fixtures
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM matches 
                WHERE match_id NOT IN (SELECT match_id FROM odds)
            """)
            pending_odds = cursor.fetchone()[0]
            
        log.info("=" * 60)
        log.info(f" -> Jogos pendentes de Odds Pré-Jogo: {pending_odds} jogos")
        log.info("=" * 60)
        
        if pending_odds > 0:
            pbar_odds = tqdm(total=pending_odds, desc="Coletando Odds Pré-Jogo", unit="jogo")
            log.info("--- [ETAPA 2/2] Extração de Odds Pré-Jogo ---")
            try:
                coletar_odds(pbar=pbar_odds)
            except Exception as e:
                log.error(f"Erro na Extração de Odds Pré-Jogo: {e}")
            pbar_odds.close()
            
        log.info("--- [FINALIZAÇÃO] Exportando CSV Unificado ---")
        export_joined_csv()
        
        log.info("=" * 60)
        log.info("Pipeline Diário de Próximos Jogos Finalizado com Sucesso!")
        log.info("=" * 60)
        return

    # -------------------------------------------------------------------------
    # 📊 MODO PADRÃO / RESULTADOS (HISTÓRICO / PÓS-JOGO COMPLETO)
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
    active_leagues_count = len([k for k, v in LIGAS_FLASHSCORE.items() if v.get("ativo", True)])
    log.info(f" -> Ligas Ativas no Config:  {active_leagues_count} ligas")
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
    parser = argparse.ArgumentParser(description="REDSCORE Pipeline Runner V3")
    parser.add_argument("--mode", type=str, choices=["results", "fixtures", "daily"], default="results",
                        help="results (default: historical outcomes), fixtures (all upcoming scheduled matches), or daily (collect tomorrow, update yesterday)")
    args = parser.parse_args()
    
    run_pipeline(mode=args.mode)
