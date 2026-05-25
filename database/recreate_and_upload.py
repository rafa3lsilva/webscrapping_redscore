import os
import sys
import glob
import numpy as np
import pandas as pd
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- Setup Paths & Env ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
load_dotenv(os.path.join(BASE_DIR, ".env"))

URL = os.getenv("SUPABASE_URL")
CHAVE = os.getenv("SUPABASE_KEY")

if not URL or not CHAVE:
    raise ValueError("⚠️ SUPABASE_URL e SUPABASE_KEY não encontrados no .env")

print("🔌 Conectando à API do Supabase...")
supabase: Client = create_client(URL, CHAVE)

# Try loading the league configuration from config.leagues
try:
    from config import leagues as cfg
    LIGAS_FLASHSCORE = cfg.LIGAS_FLASHSCORE
    LIGAS_PERMITIDAS = cfg.LIGAS_PERMITIDAS
except Exception as err:
    print(f"⚠️ Erro ao carregar config.leagues: {err}. Usando fallback vazio.")
    LIGAS_FLASHSCORE = {}
    LIGAS_PERMITIDAS = []

def to_int(val):
    if val is None or pd.isna(val):
        return None
    try:
        return int(float(str(val)))
    except Exception:
        return None

def to_float(val):
    if val is None or pd.isna(val):
        return None
    try:
        return float(str(val))
    except Exception:
        return None

# Mapeamento para nomes de ligas amigáveis
mapeamento_codigos = {}
for nome_amigavel, info in LIGAS_FLASHSCORE.items():
    code = info.get("league_code")
    if code:
        mapeamento_codigos[code.strip().upper()] = nome_amigavel

def obter_nome_amigavel(row):
    div_val = str(row.get('div', '')).strip().upper()
    return mapeamento_codigos.get(div_val)

# Buscar TODOS os arquivos CSV da pasta jogos_do_dia
csv_pattern = os.path.join(BASE_DIR, "data", "jogos_do_dia", "Jogos_do_Dia_Flashscore_*.csv")
csv_files = glob.glob(csv_pattern)

print(f"📁 Encontrados {len(csv_files)} arquivos CSV para processamento.")

dados_dia_enviar = []

for csv_path in sorted(csv_files):
    filename = os.path.basename(csv_path)
    print(f"📖 Processando arquivo: {filename}...")
    try:
        df_dia = pd.read_csv(csv_path).copy()
        
        # Normaliza colunas
        df_dia.columns = df_dia.columns.str.strip().str.lower()
        
        # Mapeia colunas de odds
        df_dia = df_dia.assign(
            odd_h_ft=pd.to_numeric(df_dia.get('odd_h_ft', 0), errors='coerce'),
            odd_d_ft=pd.to_numeric(df_dia.get('odd_d_ft', 0), errors='coerce'),
            odd_a_ft=pd.to_numeric(df_dia.get('odd_a_ft', 0), errors='coerce')
        )
        
        # Odds 0 em NaN
        df_dia.loc[df_dia['odd_h_ft'] == 0, 'odd_h_ft'] = np.nan
        df_dia.loc[df_dia['odd_d_ft'] == 0, 'odd_d_ft'] = np.nan
        df_dia.loc[df_dia['odd_a_ft'] == 0, 'odd_a_ft'] = np.nan
        
        # Substitui NaNs por None
        df_dia = df_dia.replace({np.nan: None})
        df_dia = df_dia.replace({float('nan'): None})
        
        # Determina ligas amigáveis se configuradas
        if mapeamento_codigos:
            df_dia.loc[:, 'liga_original'] = df_dia['liga']
            df_dia.loc[:, 'liga'] = df_dia.apply(obter_nome_amigavel, axis=1)
            
            # Filtra ligas permitidas
            df_dia = df_dia[df_dia['liga'].notna()]
            df_dia = df_dia[df_dia['liga'].isin(LIGAS_PERMITIDAS)]
            
        for index, row in df_dia.iterrows():
            # Tratar hora
            hora = row.get("hora")
            if hora and len(str(hora)) > 5:
                hora = str(hora)[:5]
                
            # Tratar e formatar data
            data_val = row.get("data")
            data_str = None
            if data_val:
                try:
                    data_dt = pd.to_datetime(data_val, dayfirst=True, errors='coerce')
                    if pd.notna(data_dt):
                        data_str = data_dt.strftime("%Y-%m-%d")
                    else:
                        data_str = str(data_val)
                except Exception:
                    data_str = str(data_val)
                    
            if not row.get("id_jogo"):
                continue
                
            dados_dia_enviar.append({
                "id_jogo": row.get("id_jogo"),
                "data": data_str,
                "liga": row.get("liga"),
                "hora": hora,
                "home": row.get("home"),
                "away": row.get("away"),
                "link_confronto": f"https://www.flashscore.com.br/jogo/{row.get('id_jogo')}/" if row.get('id_jogo') else None,
                
                # Todas as outras colunas contidas no CSV
                "pais": row.get("pais"),
                "div": row.get("div"),
                "temporada": str(row.get("temporada")) if row.get("temporada") is not None else None,
                "rodada": row.get("rodada"),
                "odd_h_ft": to_float(row.get("odd_h_ft")),
                "odd_d_ft": to_float(row.get("odd_d_ft")),
                "odd_a_ft": to_float(row.get("odd_a_ft")),
                "odd_h_ht": to_float(row.get("odd_h_ht")),
                "odd_d_ht": to_float(row.get("odd_d_ht")),
                "odd_a_ht": to_float(row.get("odd_a_ht")),
                "over_1.5_ft": to_float(row.get("over_1.5_ft")),
                "under_1.5_ft": to_float(row.get("under_1.5_ft")),
                "over_2.5_ft": to_float(row.get("over_2.5_ft")),
                "under_2.5_ft": to_float(row.get("under_2.5_ft")),
                "over_3.5_ft": to_float(row.get("over_3.5_ft")),
                "under_3.5_ft": to_float(row.get("under_3.5_ft")),
                "btts_yes": to_float(row.get("btts_yes")),
                "btts_no": to_float(row.get("btts_no")),
                "dc_1x": to_float(row.get("dc_1x")),
                "dc_12": to_float(row.get("dc_12")),
                "dc_x2": to_float(row.get("dc_x2"))
            })
            
    except Exception as file_err:
        print(f"❌ Erro ao processar o arquivo {filename}: {file_err}")

print(f"\n✅ Total de {len(dados_dia_enviar)} jogos prontos para envio.")

# Enviar os dados em lotes (batches de 100) para evitar timeouts ou limites de payload
batch_size = 100
total_jogos = len(dados_dia_enviar)

if dados_dia_enviar:
    # 1. Enviar para a tabela jogos_do_dia_flashscore
    print("\n🚀 Iniciando upload para 'jogos_do_dia_flashscore'...")
    for i in range(0, total_jogos, batch_size):
        batch = dados_dia_enviar[i:i+batch_size]
        try:
            supabase.table('jogos_do_dia_flashscore').upsert(batch, on_conflict='id_jogo').execute()
            print(f"📤 Enviado lote {i//batch_size + 1}/{(total_jogos-1)//batch_size + 1} ({len(batch)} jogos)...")
        except Exception as e:
            print(f"❌ Erro no lote {i//batch_size + 1}: {e}")
            
    # 2. Enviar para a tabela jogos_do_dia
    print("\n🚀 Iniciando upload para 'jogos_do_dia'...")
    for i in range(0, total_jogos, batch_size):
        batch = dados_dia_enviar[i:i+batch_size]
        try:
            supabase.table('jogos_do_dia').upsert(batch, on_conflict='id_jogo').execute()
            print(f"📤 Enviado lote {i//batch_size + 1}/{(total_jogos-1)//batch_size + 1} ({len(batch)} jogos)...")
        except Exception as e:
            print(f"❌ Erro no lote {i//batch_size + 1}: {e}")
            
    print("\n🎉 SINCRO COMPLETA! Todos os jogos históricos foram recriados e enviados para o Supabase!")
else:
    print("⚠️ Nenhum jogo compatível encontrado para upload.")
