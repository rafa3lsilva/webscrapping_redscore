import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import os
import ligas_config as cfg


# ==========================
# Função para formatar datas
# ==========================
def _formatar_data(texto_data):
    if not texto_data or not isinstance(texto_data, str):
        return None

    texto_data = texto_data.strip()
    sep = "-" if "-" in texto_data else "/"
    partes = texto_data.split(sep)

    if len(partes) != 3:
        return None

    p1, p2, p3 = partes

    # Corrigir ano de 2 dígitos → decidir século
    if len(p3) == 2:
        ano = int(p3)
        if ano >= 80:
            p3 = "19" + p3
        else:
            p3 = "20" + p3

    nova_data = f"{p1}{sep}{p2}{sep}{p3}"

    try:
        if int(p1) > 12:
            data_formatada = pd.to_datetime(
                nova_data, dayfirst=True, errors="raise")
        else:
            data_formatada = pd.to_datetime(
                nova_data, dayfirst=False, errors="raise")

        return data_formatada.strftime('%d-%m-%Y')
    except Exception:
        return None


# ==========================
# Raspagem dos links dos times
# ==========================
def raspar_links_dos_times_da_liga(liga_url):
    links_dos_times = []
    edge_options = Options()
    edge_options.add_argument("--headless")
    edge_options.add_argument("--window-size=1920,1080")
    # Esta opção oculta os erros internos do Edge (linhas vermelhas)
    edge_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    edge_options.add_argument("--log-level=3")

    try:
        servico = EdgeService(EdgeChromiumDriverManager().install())
        driver = webdriver.Edge(service=servico, options=edge_options)
    except Exception as e:
        print(f"Erro ao iniciar o WebDriver: {e}")
        return []

    try:
        driver.get(liga_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#snippet--standings table tbody td.text-nowrap a"))
        )

        elementos_link = driver.find_elements(
            By.CSS_SELECTOR, "#snippet--standings table tbody td.text-nowrap a")
        for elemento in elementos_link:
            url_time = elemento.get_attribute('href')
            if url_time and url_time not in links_dos_times:
                links_dos_times.append(url_time)

        print(
            f"  -> Encontrados {len(links_dos_times)} links de equipas únicos.")
    except Exception as e:
        print(f"  -> Ocorreu um erro ao raspar os links das equipas: {e}")
    finally:
        driver.quit()

    return links_dos_times


# ==========================
# Raspagem de jogos de um time
# ==========================
def raspar_dados_time(time_url, pais, limite_jogos=50):
    jogos_raspados = []
    edge_options = Options()
    edge_options.add_argument("--headless")
    edge_options.add_argument("--window-size=1920,1080")
    # Esta opção oculta os erros internos do Edge (linhas vermelhas)
    edge_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    edge_options.add_argument("--log-level=3")

    try:
        servico = EdgeService(EdgeChromiumDriverManager().install())
        driver = webdriver.Edge(service=servico, options=edge_options)
    except Exception as e:
        print(f"Erro ao iniciar o WebDriver: {e}")
        return []

    try:
        driver.get(time_url)

        # Loop para clicar em "see more"
        while True:
            try:
                jogos_atuais = driver.find_elements(
                    By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")
                if len(jogos_atuais) >= limite_jogos:
                    break

                see_more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.CLASS_NAME, "link-see-more"))
                )
                driver.execute_script("arguments[0].click();", see_more_button)
                WebDriverWait(driver, 5).until(
                    lambda d: len(d.find_elements(
                        By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")) > len(jogos_atuais)
                )
            except Exception:
                break

        html_completo = driver.page_source
        soup = BeautifulSoup(html_completo, 'html.parser')
        match_grid = soup.find('div', class_='match-grid__bottom')

        if match_grid:
            linhas_de_jogo = match_grid.select('tbody tr')
            for linha in linhas_de_jogo[:limite_jogos]:
                try:
                    celulas = linha.find_all('td')
                    if len(celulas) > 10:
                        data = celulas[0].text.strip()
                        liga_img = celulas[1].find('img')
                        liga = liga_img['alt'] if liga_img else 'N/A'
                        time_casa = celulas[2].text.strip()
                        placar_texto = celulas[3].text.strip()
                        time_fora = celulas[4].text.strip()
                        placar_ht = celulas[5].text.strip()
                        chutes = celulas[6].text.strip()
                        chutes_gol = celulas[7].text.strip()
                        ataques = celulas[8].text.strip()
                        escanteios = celulas[9].text.strip()
                        odd_h = celulas[11].text.strip()
                        odd_d = celulas[12].text.strip()
                        odd_a = celulas[13].text.strip()

                        jogos_raspados.append({
                            "Liga": f"{pais} - {liga}",
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
                except Exception:
                    continue

    except Exception as e:
        print(f"Ocorreu um erro geral com o Selenium: {e}")
    finally:
        driver.quit()

    return jogos_raspados


# ==========================
# Processamento dos dados
# ==========================
def processar_dados_raspados(lista_de_jogos):
    jogos_processados = []
    for jogo in lista_de_jogos:
        if jogo is None:
            continue
        try:
            data_padronizada = _formatar_data(jogo['Data'])
            if data_padronizada is None:
                continue


            # 1. Normaliza o nome da liga raspada
            liga = " ".join(jogo['Liga'].split()).title()

            # 2. Prepara a sua lista de ligas permitidas para uma comparação sem erros de maiúsculas/minúsculas
            ligas_permitidas_lower = {l.lower() for l in cfg.LIGAS_PERMITIDAS}

            # 3. Linha de depuração para ver o que está a ser comparado
            print(f"A verificar se '{liga.lower()}' está em {ligas_permitidas_lower}...")

            # 4. Verificando os nomes das ligas permitidas
            if liga.lower() not in ligas_permitidas_lower:
                # Linha de depuração para ver o que é rejeitado
                print(f" -> REJEITADA: '{liga}'")
                continue

            placar_ft = [int(p.strip()) for p in jogo['Placar_FT'].split('-')]
            placar_ht = [int(p.strip()) for p in jogo['Placar_HT'].split('-')]
            chutes = [int(p.strip()) for p in jogo['Chutes'].split('-')]
            chutes_gol = [int(p.strip())
                          for p in jogo['Chutes_Gol'].split('-')]
            ataques = [int(p.strip()) for p in jogo['Ataques'].split('-')]
            escanteios = [int(p.strip())
                          for p in jogo['Escanteios'].split('-')]

            odd_h = float(jogo['Odd_H_str'].replace(",", ".")) if jogo['Odd_H_str'].replace(
                ".", "", 1).isdigit() else 0.0
            odd_d = float(jogo['Odd_D_str'].replace(",", ".")) if jogo['Odd_D_str'].replace(
                ".", "", 1).isdigit() else 0.0
            odd_a = float(jogo['Odd_A_str'].replace(",", ".")) if jogo['Odd_A_str'].replace(
                ".", "", 1).isdigit() else 0.0

            home = " ".join(jogo['Home'].split()).title()
            away = " ".join(jogo['Away'].split()).title()

            jogos_processados.append({
                "Liga": liga,
                "Data": data_padronizada,
                "Home": home,
                "Away": away,
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
            continue

    return pd.DataFrame(jogos_processados)
