import os
import sys
import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError
import unicodedata

# --- Carregar variáveis do .env ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
load_dotenv(os.path.join(BASE_DIR, ".env"))
URL = os.getenv("SUPABASE_URL")
CHAVE = os.getenv("SUPABASE_KEY")

if not URL or not CHAVE:
    raise ValueError("⚠️ SUPABASE_URL e SUPABASE_KEY não encontrados no .env")

# Conectando à API do Supabase
print("🔌 Conectando à API do Supabase...")
supabase: Client = create_client(URL, CHAVE)

from config import leagues as cfg

def normalizar_simples(texto):
    if not isinstance(texto, str): return ""
    return "".join(c for c in unicodedata.normalize('NFKD', texto) 
                  if not unicodedata.combining(c)).lower().strip()

# Obter datas (Hoje e Amanhã)
datas_agenda = [date.today(), date.today() + timedelta(days=1)]
dados_dia_enviar = []

for dia_date in datas_agenda:
    dia_str = dia_date.strftime("%d-%m-%Y")
    caminho_jogos_dia = f'data/jogos_do_dia/Jogos_do_Dia_Flashscore_{dia_str}.csv'
    
    print(f"📖 Lendo os jogos de {dia_str}: {caminho_jogos_dia}")
    if os.path.exists(caminho_jogos_dia):
        try:
            df_dia = pd.read_csv(caminho_jogos_dia).copy()
            
            # 1. Remove apenas os espaços em branco acidentais nas pontas dos nomes
            df_dia.columns = df_dia.columns.str.strip()
            
            # 2. Forçar nomes das colunas de odds e dados a serem corretos
            df_dia = df_dia.assign(
                odd_h_ft=pd.to_numeric(df_dia['odd_h_ft'], errors='coerce'),
                odd_d_ft=pd.to_numeric(df_dia['odd_d_ft'], errors='coerce'),
                odd_a_ft=pd.to_numeric(df_dia['odd_a_ft'], errors='coerce')
            )
            
            # Transforma odds que vieram como 0.0 em NaN (vazio)
            df_dia.loc[df_dia['odd_h_ft'] == 0, 'odd_h_ft'] = np.nan
            df_dia.loc[df_dia['odd_d_ft'] == 0, 'odd_d_ft'] = np.nan
            df_dia.loc[df_dia['odd_a_ft'] == 0, 'odd_a_ft'] = np.nan
            
            # Substituir NaNs por None (null no Supabase)
            df_dia = df_dia.replace({np.nan: None})
            df_dia = df_dia.replace({float('nan'): None})
            
            # Criamos um mapeamento para converter os códigos de liga (div) dos CSVs para os nomes amigáveis oficiais
            mapeamento_codigos = {}
            for nome_amigavel, info in cfg.LIGAS_FLASHSCORE.items():
                code = info.get("league_code")
                if code:
                    mapeamento_codigos[code.strip().upper()] = nome_amigavel
            
            def obter_nome_amigavel(row):
                div_val = str(row.get('div', '')).strip().upper()
                return mapeamento_codigos.get(div_val)
                
            # Salva o nome da liga original para referência, mas usa o amigável oficial para o banco
            df_dia.loc[:, 'liga_original'] = df_dia['liga']
            df_dia.loc[:, 'liga'] = df_dia.apply(obter_nome_amigavel, axis=1)
            
            # Filtra apenas os jogos de ligas mapeadas e que estão nas permitidas
            df_dia = df_dia[df_dia['liga'].notna()]
            df_dia = df_dia[df_dia['liga'].isin(cfg.LIGAS_PERMITIDAS)]
            
            # Mapeia as colunas
            for index, row in df_dia.iterrows():
                hora = row.get("hora")
                if hora and len(str(hora)) > 5:
                    hora = str(hora)[:5]
                
                # Tratar e formatar a data de DD/MM/YYYY para YYYY-MM-DD
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
                        
                dados_dia_enviar.append({
                    "id_jogo": row.get("id_jogo"),
                    "data": data_str,
                    "liga": row.get("liga"),
                    "hora": hora,
                    "home": row.get("home"),
                    "away": row.get("away"),
                    "Odd_H": row.get("odd_h_ft"),
                    "Odd_D": row.get("odd_d_ft"),
                    "Odd_A": row.get("odd_a_ft"),
                    "link_confronto": f"https://www.flashscore.com.br/jogo/{row.get('id_jogo')}/" if row.get('id_jogo') else None
                })
                
        except Exception as e:
            print(f"❌ Erro ao ler jogos do dia {dia_str}: {e}")
    else:
        print(f"⚠️ Arquivo de jogos do dia não encontrado em: {caminho_jogos_dia}. Pulando.")

print(f"🚀 Enviando {len(dados_dia_enviar)} jogos no total para a nuvem na tabela 'jogos_do_dia_flashscore'...")
if dados_dia_enviar:
    try:
        resposta = supabase.table('jogos_do_dia_flashscore').upsert(
            dados_dia_enviar,
            on_conflict='id_jogo'
        ).execute()
        print("✅ SUCESSO! Tabela 'jogos_do_dia_flashscore' atualizada no Supabase!")
    except Exception as e:
        print(f"❌ Erro ao enviar jogos do dia para a nuvem: {e}")
else:
    print("⚠️ Nenhum jogo do dia compatível com as ligas permitidas para enviar.")

# --- Sincronizar o novo banco flashscore_v3.db com o Supabase ---
print("\n🔄 Iniciando sincronização da tabela avançada dados_flashscore_v3 no Supabase...")
try:
    from database.sync_supabase_v3 import sync_data
    sync_data(full=False)
except Exception as e:
    print(f"❌ Erro ao rodar sync_supabase_v3: {e}")
