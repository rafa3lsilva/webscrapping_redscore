import pandas as pd
import data as dt
from datetime import date, timedelta
import ligas_config as cfg
import os
import logging
import sqlite3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import warnings
warnings.filterwarnings('ignore')


# Definindo a data de amanhã
dia = date.today()+timedelta(days=1)

# ======================================== 
# Configuração de Logging e Banco de Dados
# ========================================
logging.basicConfig(
    filename="coletor.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

log = logging.getLogger(__name__)
log.info("Coletor iniciado")

NOME_DB = "dados.db"

# ================================
# Funções de Banco de Dados e CSV
# ================================
def inicializar_banco(nome_db=NOME_DB):
    conn = sqlite3.connect(nome_db)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jogos (
        Data TEXT, Home TEXT, Away TEXT, Liga TEXT, H_Gols_FT INTEGER, A_Gols_FT INTEGER,
        H_Gols_HT INTEGER, A_Gols_HT INTEGER, H_Chute INTEGER, A_Chute INTEGER,
        H_Chute_Gol INTEGER, A_Chute_Gol, H_Ataques INTEGER, A_Ataques INTEGER,
        H_Escanteios INTEGER, A_Escanteios INTEGER, Odd_H REAL, Odd_D REAL, Odd_A REAL,
        PRIMARY KEY (Data, Home, Away)
    )""")
    conn.commit()
    conn.close()

# Função para salvar DataFrame no banco de dados
def salvar_no_banco(df, nome_db=NOME_DB):
    if df.empty:
        return
    conn = sqlite3.connect(nome_db)
    df.to_sql('jogos', conn, if_exists='append', index=False)
    conn.close()
    logging.info(
        f"Dados salvos/atualizados na tabela 'jogos' ({len(df)} linhas).")

# Função para carregar jogos existentes do banco de dados
def carregar_jogos_existentes(nome_db=NOME_DB):
    if not os.path.exists(nome_db):
        return set()
    conn = sqlite3.connect(nome_db)
    jogos = {tuple(row) for row in conn.cursor().execute(
        "SELECT Data, Home, Away FROM jogos")}
    conn.close()
    return jogos

# Função para exportar dados do banco de dados para um arquivo CSV
def exportar_para_csv(nome_db=NOME_DB, nome_csv="dados_redscore.csv"):
    conn = sqlite3.connect(nome_db)
    df = pd.read_sql_query("SELECT * FROM jogos", conn)
    conn.close()
    df.to_csv(nome_csv, index=False)
    print(
        f"\n✅ Exportado histórico de jogos para {nome_csv} ({len(df)} linhas)")

# Função para exportar jogos de amanhã para um arquivo CSV
def exportar_jogos_amanha_para_csv(lista_de_jogos, nome_csv=f"jogos_do_dia/Jogos_do_Dia_RedScore_{dia}.csv"):
    """
    Converte a lista de jogos de amanhã para um DataFrame e salva como CSV.
    """
    if not lista_de_jogos:
        print("Nenhuma agenda de jogos de amanhã para exportar.")
        return

    df = pd.DataFrame(lista_de_jogos)
    df.to_csv(nome_csv, index=False, encoding='utf-8')
    print(
        f"✅ Exportada a agenda de próximos jogos para {nome_csv} ({len(df)} linhas)")
    logging.info(
        "Exportada agenda de próximos jogos para %s (%d linhas).", nome_csv, len(df))


# =====================
# Rotina Diária Noturna
# =====================
def rotina_diaria_noturna():
    inicializar_banco()
    logging.info("--- Iniciando rotina diária de atualização direcionada ---")

    print("--- Fase 1: Coletando agenda de amanhã ---")
    url_amanha = "https://redscores.com/pt-br/futebol/amanha"
    jogos_amanha = dt.raspar_jogos_de_amanha(
        url_amanha, cfg.LIGAS_PERMITIDAS)

    if not jogos_amanha:
        print("Nenhum jogo encontrado para amanhã nas ligas permitidas. Rotina concluída.")
        logging.info("Nenhum jogo encontrado para amanhã.")
        return
    exportar_jogos_amanha_para_csv(jogos_amanha)

    print(
        f"\n--- Fase 2: Obtendo links das equipas de {len(jogos_amanha)} confrontos ---")

    equipas_a_visitar = {}
    for jogo in tqdm(jogos_amanha, desc="Verificando Confrontos"):
        link_confronto = jogo['link_confronto']
        link_home, link_away = dt.obter_links_equipes_confronto(
            link_confronto)
        liga_correta = jogo['liga']

        if link_home and link_away:
            equipas_a_visitar[link_home] = liga_correta
            equipas_a_visitar[link_away] = liga_correta

    if not equipas_a_visitar:
        print("Não foi possível extrair links de equipas. Rotina concluída.")
        logging.warning(
            "Não foi possível extrair links de equipas das páginas de confronto.")
        return

    print(
        f"\n--- Fase 3: Atualizando o histórico de {len(equipas_a_visitar)} equipas ---")

    jogos_existentes = carregar_jogos_existentes()
    todos_os_jogos_novos = []

    ligas_permitidas = cfg.LIGAS_PERMITIDAS

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(dt.raspar_dados_time, url, liga_correta, jogos_existentes, ligas_permitidas, cfg.LIMITE_JOGOS_POR_TIME): (
            url, liga_correta) for url, liga_correta in equipas_a_visitar.items()}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Atualizando Histórico das Equipas"):
            try:
                todos_os_jogos_novos.extend(future.result())
            except Exception as e:
                logging.error(f"Erro ao processar URL {futures[future]}: {e}")

    if todos_os_jogos_novos:
            print(
                f"\n--- Fase 4: Processando e salvando {len(todos_os_jogos_novos)} jogos raspados ---")
            df_novos_jogos = dt.processar_dados_raspados(todos_os_jogos_novos)

            if df_novos_jogos.empty:
                print(
                    "AVISO: Nenhum dos jogos raspados pôde ser processado com sucesso. A saltar o salvamento.")
                logging.warning(
                    "Nenhum jogo foi processado com sucesso após a raspagem.")
            else:
                df_novos_jogos.drop_duplicates(
                    subset=["Data", "Home", "Away"], inplace=True, keep='last')

                jogos_existentes_df = pd.DataFrame(
                    list(jogos_existentes), columns=["Data", "Home", "Away"])
                if not jogos_existentes_df.empty:
                    df_novos_jogos = df_novos_jogos.merge(jogos_existentes_df, on=[
                                                          "Data", "Home", "Away"], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

                if not df_novos_jogos.empty:
                    salvar_no_banco(df_novos_jogos)
                    print(
                        f"✅ {len(df_novos_jogos)} novos resultados salvos no banco de dados.")
                else:
                    print("Todos os jogos processados já existiam no banco de dados.")
    else:
        print("\nNenhum resultado novo encontrado para as equipas de amanhã.")

    exportar_para_csv()
    logging.info("--- Rotina diária concluída ---")
    print("\n--- Rotina diária concluída ---")

# ============================
# Início da execução da rotina
# ============================
if __name__ == "__main__":
    rotina_diaria_noturna()
