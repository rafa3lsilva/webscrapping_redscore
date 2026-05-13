import os
from datetime import datetime
import numpy as np
import pandas as pd
from supabase import create_client, Client
from datetime import date, timedelta, datetime
from postgrest.exceptions import APIError
from dotenv import load_dotenv

# --- Carregar variáveis do .env ---
load_dotenv()
URL = os.getenv("SUPABASE_URL")
CHAVE = os.getenv("SUPABASE_KEY")

if not URL or not CHAVE:
    raise ValueError("⚠️ SUPABASE_URL e SUPABASE_KEY não encontrados no .env")
dia = date.today() + timedelta(days=1)

print("1. Conectando à API do Supabase...")
supabase: Client = create_client(URL, CHAVE)

from config import leagues as cfg
import unicodedata

def normalizar_simples(texto):
    if not isinstance(texto, str): return ""
    return "".join(c for c in unicodedata.normalize('NFKD', texto) 
                  if not unicodedata.combining(c)).lower().strip()

print("2. Lendo os dados locais do banco principal...")
df_jogos = pd.read_csv('output/dados_redscore.csv')

# Substitui NaN por None para não quebrar a API do Supabase
df_jogos = df_jogos.replace({float('nan'): None})

# Filtra apenas as ligas permitidas (robusto a maiúsculas/minúsculas e acentos)
ligas_permitidas_norm = {normalizar_simples(l) for l in cfg.LIGAS_PERMITIDAS}
df_jogos = df_jogos[df_jogos['Liga'].apply(lambda x: normalizar_simples(x) in ligas_permitidas_norm)]

# Transforma a tabela em uma lista de dicionários pronta para o Supabase
dados_para_enviar = df_jogos.to_dict(orient='records')

print(f"3. Enviando {len(dados_para_enviar)} jogos para a nuvem...")

# try/except para capturar o erro de duplicação
try:
    resposta = supabase.table('dados_redscore').upsert(
        dados_para_enviar,
        on_conflict='Data,Home,Away'
    ).execute()

    print("✅ SUCESSO! Banco 'dados_redscore' migrado! Jogos novos inseridos!")

except APIError as e:
    # O código '23505' é o padrão do PostgreSQL para "dados duplicados / violação de chave única"
    if e.code == '23505':
        print("⚠️ AVISO: Os dados não foram inseridos porque já existem na nuvem (Dados Duplicados).")
    else:
        # Se for outro erro de API, ele mostra qual é
        print(f"❌ Erro na API do Supabase: {e.message}")

except Exception as e:
    # Captura qualquer outro erro genérico
    print(f"❌ Erro inesperado: {e}")

# update jogos do dia
#nome_arquivo = f'jogos_do_dia/Jogos_do_Dia_RedScore_{dia}.csv'

print("1. A ler os jogos do dia...")
# Substitua pelo ficheiro do dia
df_dia = pd.read_csv(f'output/jogos_do_dia/Jogos_do_Dia_RedScore_{dia}.csv')

# 1. Remove apenas os espaços em branco acidentais nas pontas dos nomes
df_dia.columns = df_dia.columns.str.strip()

# 2. Se o scraper estiver salvando 'liga' com 'L' minúsculo, forçamos o nome correto
df_dia.rename(columns={'liga': 'Liga'}, inplace=True)

print("2. A aplicar regras de negócio (POSTP e Odds)...")
# Força as odds a serem numéricas. Textos estranhos viram NaN (vazio)
df_dia = df_dia.assign(
    Odd_H=pd.to_numeric(df_dia['Odd_H'], errors='coerce'),
    Odd_D=pd.to_numeric(df_dia['Odd_D'], errors='coerce'),
    Odd_A=pd.to_numeric(df_dia['Odd_A'], errors='coerce')
)

# Transforma odds que vieram como 0.0 em NaN (vazio) para permitir a inserção manual
df_dia.loc[df_dia['Odd_H'] == 0, 'Odd_H'] = np.nan
df_dia.loc[df_dia['Odd_D'] == 0, 'Odd_D'] = np.nan
df_dia.loc[df_dia['Odd_A'] == 0, 'Odd_A'] = np.nan

# IMPORTANTE: Já não usamos o dropna()! Mantemos os jogos com odds vazias.
# Apenas convertemos os NaN do Pandas para None (que vira null no Supabase)
#df_dia = df_dia.where(pd.notna(df_dia), None)
df_dia = df_dia.replace({np.nan: None})

# Filtra apenas as ligas permitidas na agenda também
df_dia = df_dia[df_dia['Liga'].apply(lambda x: normalizar_simples(x) in ligas_permitidas_norm)]

# Converter para dicionário
jogos_do_dia = df_dia.to_dict(orient='records')

print(f"3. A enviar {len(jogos_do_dia)} jogos para a nuvem...")

# 1. Troca qualquer espaço vazio (NaN) por None (null), igual fizemos no banco principal
df_dia = df_dia.replace({float('nan'): None})

# 2. Constrói a lista "na unha", garantindo os nomes EXATOS das colunas do PostgreSQL
dados_dia_enviar = []
for index, row in df_dia.iterrows():
    dados_dia_enviar.append({
        "data": row.get("Data", row.get("data")),
        "liga": row.get("Liga", row.get("liga")),
        "hora": row.get("Hora", row.get("hora")),
        "home": row.get("Home", row.get("home")),
        "away": row.get("Away", row.get("away")),
        "Odd_H": row.get("Odd_H"),
        "Odd_D": row.get("Odd_D"),
        "Odd_A": row.get("Odd_A"),
        "link_confronto": row.get("Link_confronto", row.get("link_confronto"))
    })
try:
    # Fazemos o upsert com base no link.
    # Se for um jogo adiado que foi remarcado, ele atualiza a data e a hora!
    resposta = supabase.table('jogos_do_dia').upsert(
        dados_dia_enviar,
        on_conflict='data,home,away'
    ).execute()

    print("✅ SUCESSO! Base jogos do dia atualizada.")

except Exception as e:
    print(f"❌ Erro ao enviar para a nuvem: {e}")
