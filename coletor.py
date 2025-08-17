import pandas as pd
import data as dt
import ligas as lg
import os
import time

# Nome do ficheiro que servirá como nossa base de dados
NOME_FICHEIRO_DADOS = "dados_historicos.csv"

def coletar_novos_dados():
    """
    Função principal do robô coletor.
    """
    print("--- A iniciar o processo de coleta de dados ---")

    # --- Passo 1: Carregar os Dados Históricos ---
    if os.path.exists(NOME_FICHEIRO_DADOS):
        df_historico = pd.read_csv(NOME_FICHEIRO_DADOS)
        print(
            f"Base de dados histórica encontrada com {len(df_historico)} jogos.")
    else:
        df_historico = pd.DataFrame()
        print("Nenhuma base de dados histórica encontrada. Um novo ficheiro será criado.")

    # --- Passo 2: Criar Identificadores Únicos para os Jogos Existentes ---
    # Usamos uma combinação de Data, Time da Casa e Time Visitante como chave única.
    if not df_historico.empty:
        identificadores_existentes = set(
            df_historico['Data'] + '-' + df_historico['Home'] + '-' + df_historico['Away'])
    else:
        identificadores_existentes = set()

    # --- Passo 3: Percorrer as Ligas e Raspar Novos Dados ---
    lista_de_ligas = lg.links_ligas()
    todos_os_jogos_novos = []

    for url_liga in lista_de_ligas:
        print(f"\nA raspar a liga: {url_liga}")
        # A função raspar_dados_time foi adaptada para raspar de páginas de ligas
        # Ela agora precisa de uma lógica para encontrar os links dos jogos e visitá-los.
        # Vamos assumir que temos uma função 'raspar_jogos_da_liga' em data.py

        # NOTA: Precisaremos de uma nova função de scraping para páginas de liga.
        # Por agora, vamos simular esta parte e focar na lógica de salvar.

        # SIMULAÇÃO: Esta linha seria substituída pela chamada real ao scraper da liga
        jogos_raspados_da_liga = dt.raspar_dados_time(
            url_liga, limite_jogos=50)  # Usando a função antiga como placeholder

        if not jogos_raspados_da_liga:
            print("Nenhum jogo encontrado nesta liga. A passar para a próxima.")
            continue

        for jogo in jogos_raspados_da_liga:
            identificador_jogo = f"{jogo['Data']}-{jogo['Home']}-{jogo['Away']}"

            # Verifica se o jogo já existe na nossa base de dados
            if identificador_jogo not in identificadores_existentes:
                todos_os_jogos_novos.append(jogo)
                # Adiciona ao set para evitar duplicados na mesma execução
                identificadores_existentes.add(identificador_jogo)
                print(
                    f"  -> Novo jogo encontrado: {jogo['Home']} vs {jogo['Away']}")

        # Pausa de 5 segundos entre cada liga para sermos cordiais
        time.sleep(5)

    # --- Passo 4: Processar e Salvar os Novos Dados ---
    if todos_os_jogos_novos:
        print(
            f"\nForam encontrados {len(todos_os_jogos_novos)} jogos novos no total.")

        # Processa os dados raspados para o formato correto
        df_novos_jogos = dt.processar_dados_raspados(todos_os_jogos_novos)

        # Junta o DataFrame histórico com os novos jogos
        df_final = pd.concat([df_historico, df_novos_jogos], ignore_index=True)

        # Salva o ficheiro CSV atualizado, substituindo o antigo
        df_final.to_csv(NOME_FICHEIRO_DADOS, index=False)
        print(
            f"Base de dados atualizada com sucesso! Total de {len(df_final)} jogos.")
    else:
        print("\nNenhum jogo novo encontrado. A base de dados já está atualizada.")

    print("--- Processo de coleta de dados finalizado ---")


# Executa a função principal quando o script é chamado
if __name__ == "__main__":
    coletar_novos_dados()
