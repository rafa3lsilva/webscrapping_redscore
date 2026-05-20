import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError

# --- Load env variables ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

URL = os.getenv("SUPABASE_URL")
CHAVE = os.getenv("SUPABASE_KEY")

DB_PATH = os.path.join(BASE_DIR, "data", "flashscore_v3.db")

SQL_DDL = """
-- COPIE E COLE ESTE SQL NO SQL EDITOR DO SEU SUPABASE:
CREATE TABLE IF NOT EXISTS dados_flashscore_v3 (
    id_jogo TEXT PRIMARY KEY,
    pais TEXT,
    liga_full TEXT,
    temporada TEXT,
    "Data" TIMESTAMP WITH TIME ZONE,
    "Home" TEXT,
    "Away" TEXT,
    "H_Gols_FT" DOUBLE PRECISION,
    "A_Gols_FT" DOUBLE PRECISION,
    "H_Gols_HT" DOUBLE PRECISION,
    "A_Gols_HT" DOUBLE PRECISION,
    "Odd_H" DOUBLE PRECISION,
    "Odd_D" DOUBLE PRECISION,
    "Odd_A" DOUBLE PRECISION,
    "xg_h" DOUBLE PRECISION,
    "xg_a" DOUBLE PRECISION,
    "xgot_h" DOUBLE PRECISION,
    "xgot_a" DOUBLE PRECISION,
    "H_Chute" DOUBLE PRECISION,
    "A_Chute" DOUBLE PRECISION,
    "H_Chute_Gol" DOUBLE PRECISION,
    "A_Chute_Gol" DOUBLE PRECISION,
    "H_Grandes_Chances" DOUBLE PRECISION,
    "A_Grandes_Chances" DOUBLE PRECISION,
    "H_Escanteios" DOUBLE PRECISION,
    "A_Escanteios" DOUBLE PRECISION,
    "H_Chutes_Area" DOUBLE PRECISION,
    "A_Chutes_Area" DOUBLE PRECISION
);
"""

def sync_data():
    if not URL or not CHAVE:
        print("❌ Erro: SUPABASE_URL e SUPABASE_KEY não configurados no .env do Scrapper.")
        sys.exit(1)

    if not os.path.exists(DB_PATH):
        print(f"❌ Erro: Base SQLite local não encontrada em {DB_PATH}")
        sys.exit(1)

    print("🔌 Conectando ao Supabase...")
    supabase: Client = create_client(URL, CHAVE)

    print("🔌 Conectando à base de dados SQLite local...")
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT 
        m.match_id as id_jogo,
        m.country as pais,
        m.league_full_name as liga_full,
        m.season as temporada,
        m.date as Data,
        m.home_team as Home,
        m.away_team as Away,
        m.home_score as H_Gols_FT,
        m.away_score as A_Gols_FT,
        m.home_score_ht as H_Gols_HT,
        m.away_score_ht as A_Gols_HT,
        
        o.odd_h_ft as Odd_H,
        o.odd_d_ft as Odd_D,
        o.odd_a_ft as Odd_A,
        
        s.xG_Home_FT as xg_h,
        s.xG_Away_FT as xg_a,
        s.xGOT_Home_FT as xgot_h,
        s.xGOT_Away_FT as xgot_a,
        s.Total_Shots_Home_FT as H_Chute,
        s.Total_Shots_Away_FT as A_Chute,
        s.Shots_On_Target_Home_FT as H_Chute_Gol,
        s.Shots_On_Target_Away_FT as A_Chute_Gol,
        s.Big_Chances_Home_FT as H_Grandes_Chances,
        s.Big_Chances_Away_FT as A_Grandes_Chances,
        s.Corners_Home_FT as H_Escanteios,
        s.Corners_Away_FT as A_Escanteios,
        s.Shots_Inside_Box_Home_FT as H_Chutes_Area,
        s.Shots_Inside_Box_Away_FT as A_Chutes_Area
    FROM matches m
    LEFT JOIN odds o ON m.match_id = o.match_id
    LEFT JOIN stats s ON m.match_id = s.match_id
    """
    
    df = pd.read_sql_query(query, conn).copy()
    conn.close()
    
    print(f"📖 Carregadas {len(df)} partidas do SQLite.")
    
    # Limpeza e conversões de datas
    df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['Data'])
    
    # Formatar data como string ISO para o Postgres
    df['Data'] = df['Data'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
    
    # Converter NaNs do pandas/numpy para None (virando null no Supabase)
    df = df.replace({np.nan: None})
    df = df.replace({float('nan'): None})
    
    dados_para_enviar = df.to_dict(orient='records')
    total_jogos = len(dados_para_enviar)
    print(f"🔄 Preparados {total_jogos} jogos para envio.")
    
    # Enviar em lotes (batching) de 2000 em 2000
    tamanho_lote = 2000
    
    print("\n🚀 Iniciando sincronização em lotes...")
    
    for i in range(0, total_jogos, tamanho_lote):
        lote = dados_para_enviar[i:i+tamanho_lote]
        print(f"  📤 Enviando lote {i // tamanho_lote + 1} ({len(lote)} jogos)...")
        try:
            supabase.table('dados_flashscore_v3').upsert(
                lote,
                on_conflict='id_jogo'
            ).execute()
        except APIError as e:
            err_msg = str(e.message).lower()
            if "relation" in err_msg or "não existe" in err_msg or "could not find the table" in err_msg or "schema cache" in err_msg:
                print("\n❌ ERRO: A tabela 'dados_flashscore_v3' não existe no seu Supabase!")
                print("Por favor, copie e execute o código SQL DDL abaixo no 'SQL Editor' do painel do seu Supabase:")
                print("="*60)
                print(SQL_DDL)
                print("="*60)
                sys.exit(1)
            else:
                print(f"❌ Erro de API no lote {i}: {e.message}")
                sys.exit(1)
        except Exception as e:
            print(f"❌ Erro inesperado no lote {i}: {e}")
            sys.exit(1)
            
    print("\n✅ SUCESSO! Toda a base SQLite 'flashscore_v3.db' foi sincronizada com o Supabase!")

if __name__ == "__main__":
    sync_data()
