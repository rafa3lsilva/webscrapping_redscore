import pandas as pd
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def drop_reset_index(df):
    df = df.dropna()
    df = df.reset_index(drop=True)
    df.index += 1
    return df

# Adicionado um limite padrão de 41 jogos
def raspar_dados_time(url, limite_jogos=41):
    """
    Função de web scraping que usa Selenium e para de carregar mais jogos
    quando atinge um limite especificado.
    """
    jogos_raspados = []

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    try:
        servico = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=servico, options=chrome_options)
    except Exception as e:
        print.error(f"Erro ao iniciar o WebDriver para o Chrome: {e}")
        return []

    try:
        driver.get(url)
        time.sleep(5)

        # --- Laço para listar os jogos ---
        while True:
            try:
                # Verificando quantos jogos visiveis
                jogos_atuais = driver.find_elements(
                    By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")
                print(f"Jogos carregados até agora: {len(jogos_atuais)}")

                # Verificando se já atingiu o limite
                if len(jogos_atuais) >= limite_jogos:
                    print(
                        f"Limite de {limite_jogos} jogos atingido. A parar de carregar mais.")
                    break

                # Se não atingido o limite, procura e clica no botão
                see_more_button = driver.find_element(
                    By.CLASS_NAME, "link-see-more")
                driver.execute_script("arguments[0].click();", see_more_button)

                print("Clicou em 'see more', a aguardar mais jogos...")
                time.sleep(3)
            except Exception:
                print(
                    "Botão 'see more' não encontrado. Todos os jogos foram carregados.")
                break

        print("Loop de carregamento terminado. A iniciar extração final...")
        html_completo = driver.page_source
        soup = BeautifulSoup(html_completo, 'html.parser')

        match_grid = soup.find('div', class_='match-grid__bottom')
        if match_grid:
            # Seleciona apenas as linhas do corpo da tabela (tbody) para evitar cabeçalhos
            linhas_de_jogo = match_grid.select('tbody tr')

            for linha in linhas_de_jogo[:limite_jogos]:
                try:
                    # Encontra todas as células (td) da linha
                    celulas = linha.find_all('td')

                    # Verifica se a linha tem o número mínimo de células para ser um jogo válido
                    if len(celulas) > 10:
                        data = celulas[0].text.strip()
                        liga_img = celulas[1].find('img')
                        liga = liga_img['alt'] if liga_img else 'N/A'
                        time_casa = celulas[2].text.strip()
                        placar_texto = celulas[3].text.strip()
                        time_fora = celulas[4].text.strip()
                        placar_ht = celulas[5].text.strip()

                        # Adiciona a extração das estatísticas detalhadas
                        chutes = celulas[6].text.strip()
                        chutes_gol = celulas[7].text.strip()
                        ataques = celulas[8].text.strip()
                        escanteios = celulas[9].text.strip()

                        # Extração das odds
                        odd_h = celulas[11].text.strip()
                        odd_d = celulas[12].text.strip()
                        odd_a = celulas[13].text.strip()

                        # Cria um dicionário com os dados extraídos (como texto)
                        jogos_raspados.append({
                            "Liga": liga,
                            "Data": data,
                            "Home": time_casa,
                            "Away": time_fora,
                            "Placar_FT": placar_texto,
                            "Placar_HT": placar_ht,
                            "Chutes": chutes,
                            "Chutes_Gol": chutes_gol,
                            "Ataques": ataques,
                            "Escanteios": escanteios,
                            "Odd_H_str": odd_h,
                            "Odd_D_str": odd_d,
                            "Odd_A_str": odd_a
                        })

                except Exception as e:
                    # Se uma linha específica falhar, ignora e continua
                    continue
        else:
            print("Não foi possível encontrar a grelha de jogos na página.")

    except Exception as e:
        print(f"Ocorreu um erro geral com o Selenium: {e}")
    finally:
        driver.quit()

    return jogos_raspados

def processar_dados_raspados(lista_de_jogos):
    """
    Converte os dados raspados (que são strings) para o formato final do DataFrame,
    pronto para ser usado pelas funções de análise.
    """
    jogos_processados = []
    for jogo in lista_de_jogos:
        try:
            # Separa os placares e estatísticas que estão em formato "X - Y"
            placar_ft = [int(p.strip()) for p in jogo['Placar_FT'].split('-')]
            placar_ht = [int(p.strip()) for p in jogo['Placar_HT'].split('-')]
            chutes = [int(p.strip()) for p in jogo['Chutes'].split('-')]
            chutes_gol = [int(p.strip())
                          for p in jogo['Chutes_Gol'].split('-')]
            ataques = [int(p.strip()) for p in jogo['Ataques'].split('-')]
            escanteios = [int(p.strip())
                          for p in jogo['Escanteios'].split('-')]

            # Converte as odds para float, tratando o caso de não existirem
            odd_h = float(jogo['Odd_H_str']) if jogo['Odd_H_str'].replace(
                '.', '', 1).isdigit() else 0.0
            odd_d = float(jogo['Odd_D_str']) if jogo['Odd_D_str'].replace(
                '.', '', 1).isdigit() else 0.0
            odd_a = float(jogo['Odd_A_str']) if jogo['Odd_A_str'].replace(
                '.', '', 1).isdigit() else 0.0

            jogos_processados.append({
                "Liga": jogo['Liga'],
                "Home": jogo['Home'],
                "Away": jogo['Away'],
                "H_Gols_FT": placar_ft[0],
                "A_Gols_FT": placar_ft[1],
                "H_Gols_HT": placar_ht[0],
                "A_Gols_HT": placar_ht[1],
                "H_Chute": chutes[0],
                "A_Chute": chutes[1],
                "H_Chute_Gol": chutes_gol[0],
                "A_Chute_Gol": chutes_gol[1],
                "H_Ataques": ataques[0],
                "A_Ataques": ataques[1],
                "H_Escanteios": escanteios[0],
                "A_Escanteios": escanteios[1],
                "Odd_H": odd_h,
                "Odd_D": odd_d,
                "Odd_A": odd_a
            })
        except (ValueError, IndexError):
            # Ignora jogos que não tenham todas as estatísticas completas
            continue

    return pd.DataFrame(jogos_processados)