import pandas as pd
import data as dt
import ligas as lg
import os
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from urllib.parse import urlparse

# Configuração do log
logging.basicConfig(
    filename="coletor.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

NOME_DB = "dados.db"

# ===============================
# Inicializa banco com restrição
# ===============================


def inicializar_banco(nome_db=NOME_DB):
    conn = sqlite3.connect(nome_db)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jogos (
        Data TEXT,
        Home TEXT,
        Away TEXT,
        Liga TEXT,
        H_Gols_FT INTEGER,
        A_Gols_FT INTEGER,
        H_Gols_HT INTEGER,
        A_Gols_HT INTEGER,
        H_Chute INTEGER,
        A_Chute INTEGER,
        H_Chute_Gol INTEGER,
        A_Chute_Gol INTEGER,
        H_Ataques INTEGER,
        A_Ataques INTEGER,
        H_Escanteios INTEGER,
        A_Escanteios INTEGER,
        Odd_H REAL,
        Odd_D REAL,
        Odd_A REAL,
        PRIMARY KEY (Data, Home, Away)
    )
    """)

    conn.commit()
    conn.close()


# ===============================
# Salvar dados no SQLite sem duplicar
# ===============================
def salvar_no_banco(df, nome_db=NOME_DB):
    conn = sqlite3.connect(nome_db)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
        INSERT OR REPLACE INTO jogos (
            Data, Home, Away, Liga,
            H_Gols_FT, A_Gols_FT, H_Gols_HT, A_Gols_HT,
            H_Chute, A_Chute, H_Chute_Gol, A_Chute_Gol,
            H_Ataques, A_Ataques, H_Escanteios, A_Escanteios,
            Odd_H, Odd_D, Odd_A
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["Data"], row["Home"], row["Away"], row["Liga"],
            row["H_Gols_FT"], row["A_Gols_FT"], row["H_Gols_HT"], row["A_Gols_HT"],
            row["H_Chute"], row["A_Chute"], row["H_Chute_Gol"], row["A_Chute_Gol"],
            row["H_Ataques"], row["A_Ataques"], row["H_Escanteios"], row["A_Escanteios"],
            row["Odd_H"], row["Odd_D"], row["Odd_A"]
        ))

    conn.commit()
    conn.close()
    logging.info("Dados salvos no banco %s (%d linhas).", nome_db, len(df))


# ===============================
# Exportar banco para CSV
# ===============================
def exportar_para_csv(nome_db=NOME_DB, nome_csv="dados_redscore.csv"):
    conn = sqlite3.connect(nome_db)
    query = "SELECT DISTINCT * FROM jogos"
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv(nome_csv, index=False, encoding="utf-8")
    logging.info("Exportado para CSV: %s (%d linhas).", nome_csv, len(df))
    print(f"✅ Exportado para {nome_csv} ({len(df)} linhas)")


# ===============================
# Extrair país da URL da liga
# ===============================
def extrair_pais(liga_url):
    path_parts = urlparse(liga_url).path.split("/")
    return path_parts[3].title() if len(path_parts) > 3 else "Unknown"


# ===============================
# Processo principal de coleta
# ===============================
def coletar_novos_dados():
    inicializar_banco(NOME_DB)
    logging.info("--- Iniciando coleta de dados ---")

    lista_de_urls_de_ligas = lg.links_ligas()
    todos_os_links_de_equipas = []

    for url_liga in tqdm(lista_de_urls_de_ligas, desc="Processando Ligas"):
        pais = extrair_pais(url_liga)
        links_das_equipas = dt.raspar_links_dos_times_da_liga(url_liga)
        todos_os_links_de_equipas.extend(
            [(pais, url) for url in links_das_equipas])

    todos_os_links_de_equipas = sorted(list(set(todos_os_links_de_equipas)))
    logging.info("Encontradas %d equipes únicas.",
                 len(todos_os_links_de_equipas))

    todos_os_jogos_novos = []

    def processar_time(pais_url):
        pais, url_time = pais_url
        return dt.raspar_dados_time(url_time, pais, limite_jogos=200)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(processar_time, pu)                   : pu for pu in todos_os_links_de_equipas}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Raspando Equipes"):
            try:
                jogos_raspados_da_equipa = future.result()
                todos_os_jogos_novos.extend(jogos_raspados_da_equipa)
            except Exception as e:
                logging.error("Erro ao processar equipe %s: %s",
                              futures[future], e)

    if todos_os_jogos_novos:
        logging.info("Foram encontrados %d jogos novos.",
                     len(todos_os_jogos_novos))
        df_novos_jogos = dt.processar_dados_raspados(todos_os_jogos_novos)
        df_novos_jogos.drop_duplicates(
            subset=["Data", "Home", "Away"], inplace=True)
        salvar_no_banco(df_novos_jogos)
    else:
        logging.info("Nenhum jogo novo encontrado.")

    exportar_para_csv()
    logging.info("--- Fim da coleta ---")


if __name__ == "__main__":
    coletar_novos_dados()
