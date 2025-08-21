import pandas as pd
import data as dt
import ligas_config as cfg
import os
import logging
import sqlite3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse

# --- CONFIGURAÇÃO ---
logging.basicConfig(filename="coletor.log", level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
NOME_DB = "dados.db"

# --- FUNÇÕES DE BANCO DE DADOS E CSV ---


def inicializar_banco(nome_db=NOME_DB):
    conn = sqlite3.connect(nome_db)
    cursor = conn.cursor()
    # Agora só precisamos de uma tabela
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


def salvar_no_banco(df, nome_db=NOME_DB):
    if df.empty:
        return
    conn = sqlite3.connect(nome_db)
    # Usamos 'replace' para garantir que os jogos existentes são atualizados com os dados mais recentes
    df.to_sql('jogos', conn, if_exists='append', index=False)
    conn.close()
    logging.info(
        f"Dados salvos/atualizados na tabela 'jogos' ({len(df)} linhas).")


def carregar_jogos_existentes(nome_db=NOME_DB):
    if not os.path.exists(nome_db):
        return set()
    conn = sqlite3.connect(nome_db)
    jogos = {tuple(row) for row in conn.cursor().execute(
        "SELECT Data, Home, Away FROM jogos")}
    conn.close()
    return jogos


def extrair_pais(time_url):
    try:
        return urlparse(time_url).path.split('/')[3].title()
    except:
        return "Unknown"


def exportar_para_csv(nome_db=NOME_DB, nome_csv="dados_redscore.csv"):
    conn = sqlite3.connect(nome_db)
    df = pd.read_sql_query("SELECT * FROM jogos", conn)
    conn.close()
    df.to_csv(nome_csv, index=False)
    print(
        f"\n✅ Exportado histórico de jogos para {nome_csv} ({len(df)} linhas)")


def exportar_jogos_amanha_para_csv(lista_de_jogos, nome_csv="proximos_jogos.csv"):
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


# --- ROTINA PRINCIPAL ---
def rotina_diaria_noturna():
    inicializar_banco()
    logging.info("--- Iniciando rotina diária de atualização direcionada ---")

    # 1. Obter a agenda de confrontos de amanhã
    print("--- Fase 1: Coletando agenda de amanhã ---")
    url_amanha = "https://redscores.com/pt-br/futebol/amanha"
    jogos_agendados = dt.raspar_jogos_de_amanha(
        url_amanha, cfg.LIGAS_PERMITIDAS)

    if not jogos_agendados:
        print("Nenhum jogo encontrado para amanhã nas ligas permitidas. Rotina concluída.")
        logging.info("Nenhum jogo encontrado para amanhã.")
        return
    exportar_jogos_amanha_para_csv(jogos_agendados)

    print(
        f"\n--- Fase 2: Obtendo links das equipas de {len(jogos_agendados)} confrontos ---")

    # 2. Visitar cada confronto para obter os links das equipas
    equipas_a_visitar = {}
    for jogo in tqdm(jogos_agendados, desc="Verificando Confrontos"):
        link_confronto = jogo['link_confronto']
        link_home, link_away = dt.obter_links_de_equipa_do_confronto(
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

    # 3. Atualizar o histórico de cada equipa relevante
    jogos_existentes = carregar_jogos_existentes()
    todos_os_jogos_novos = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(dt.raspar_dados_time, url, liga_correta, jogos_existentes, 20): (
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

            # --- VERIFICAÇÃO DE SEGURANÇA ADICIONADA AQUI ---
            if df_novos_jogos.empty:
                print(
                    "AVISO: Nenhum dos jogos raspados pôde ser processado com sucesso. A saltar o salvamento.")
                logging.warning(
                    "Nenhum jogo foi processado com sucesso após a raspagem.")
            else:
                # O resto da lógica só executa se o DataFrame não estiver vazio
                df_novos_jogos.drop_duplicates(
                    subset=["Data", "Home", "Away"], inplace=True, keep='last')

                jogos_existentes_df = pd.DataFrame(
                    list(jogos_existentes), columns=["Data", "Home", "Away"])
                if not jogos_existentes_df.empty:
                    df_novos_jogos = df_novos_jogos.merge(jogos_existentes_df, on=[
                                                          "Data", "Home", "Away"], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

                # Verificação final para garantir que ainda há jogos novos após o merge
                if not df_novos_jogos.empty:
                    salvar_no_banco(df_novos_jogos)
                    print(
                        f"✅ {len(df_novos_jogos)} novos resultados salvos no banco de dados.")
                else:
                    print("Todos os jogos processados já existiam no banco de dados.")
    else:
        print("\nNenhum resultado novo encontrado para as equipas de amanhã.")

    exportar_para_csv()
    #exportar_jogos_amanha_para_csv(jogos_agendados)
    logging.info("--- Rotina diária concluída ---")
    print("\n--- Rotina diária concluída ---")


if __name__ == "__main__":
    rotina_diaria_noturna()
