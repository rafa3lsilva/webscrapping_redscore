import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import ligas_config as cfg
import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

log = logging.getLogger(__name__)

# ==========================
# Funções Auxiliares
# ==========================
def _iniciar_driver():
    """Inicializa e retorna uma instância do WebDriver de forma 'furtiva' e silenciosa."""
    edge_options = Options()
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0"
    edge_options.add_argument(f'user-agent={user_agent}')
    edge_options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
    edge_options.add_experimental_option('useAutomationExtension', False)
    edge_options.add_argument("--disable-blink-features=AutomationControlled")
    edge_options.add_argument("--start-maximized")
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--headless")

    try:
        caminho_driver_local = "./msedgedriver.exe"
        if not os.path.exists(caminho_driver_local):
            print(
                f"ERRO: O ficheiro '{caminho_driver_local}' não foi encontrado.")
            return None
        servico = EdgeService(executable_path=caminho_driver_local)
        driver = webdriver.Edge(service=servico, options=edge_options)
        return driver
    except Exception as e:
        print(f"Erro ao iniciar o WebDriver local: {e}")
        return None

# ==========================
# Função para formatar datas
# ==========================


def _formatar_data(texto_data):
    """
    Formata uma string de data para o formato DD-MM-YYYY, lidando
    corretamente com o formato americano (MM-DD-YYYY) do site.
    """
    if not texto_data or not isinstance(texto_data, str):
        return None
    try:
        data_obj = pd.to_datetime(texto_data)

        return data_obj.strftime('%d-%m-%Y')
    except Exception:
        return None

# ==========================
# Função para converter strings de estatísticas
# ==========================
def _converter_stat_para_int(stat_string):
    """Converte uma string 'A - B' para uma lista [A, B]. Retorna [0, 0] em caso de falha."""
    if not isinstance(stat_string, str) or '-' not in stat_string:
        return [0, 0]
    try:
        partes = [int(p.strip()) for p in stat_string.split('-')]
        return partes if len(partes) == 2 else [0, 0]
    except (ValueError, IndexError):
        return [0, 0]
    
# ==========================
# Função de Raspagem
# ==========================

def raspar_jogos_de_amanha(url_amanha, ligas_permitidas_set):
    """
    Versão FINAL OTIMIZADA COM LOGGING.
    Expande ligas colapsadas verificando o estado real do botão (rotate(0deg))
    e espera até que os jogos estejam carregados antes de prosseguir.
    """
    lista_de_jogos = []
    driver = _iniciar_driver()
    if not driver:
        log.error("Falha ao iniciar o driver")
        return lista_de_jogos

    try:
        driver.get(url_amanha)
        log.info("Página aberta. A procurar por banner de cookies...")
        try:
            seletor_botao_aceitar = "div.cookieinfo-close"
            botao_aceitar = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, seletor_botao_aceitar))
            )
            botao_aceitar.click()
            log.info("Banner de cookies aceite com sucesso.")
            time.sleep(2)
        except Exception:
            log.info("Nenhum banner de cookies encontrado. A continuar...")

        log.info("A aguardar para a página estabilizar...")
        time.sleep(8)

        # Normaliza ligas permitidas
        ligas_normalizadas = {l.lower() for l in ligas_permitidas_set}

        # --- FASE A: ENCONTRAR E EXPANDIR LIGAS ---
        log.info("Fase A: Procurando e expandindo ligas de interesse...")
        soup_inicial = BeautifulSoup(driver.page_source, 'html.parser')
        blocos_de_liga_inicial = soup_inicial.find_all(
            'div', id=lambda x: x and x.startswith('league_'))

        for bloco in blocos_de_liga_inicial:
            try:
                pais_el = bloco.select_one("span.d-block.d-md-inline")
                pais = pais_el.text.strip() if pais_el else ''
                liga_el = bloco.select_one("span.font-bold")
                liga = liga_el.text.strip() if liga_el else ''
                nome_liga_completo = f"{pais} - {liga}" if pais else liga

                if nome_liga_completo.lower() not in ligas_normalizadas:
                    continue

                id_do_bloco = bloco['id']
                seletor_do_botao = f"#{id_do_bloco} a.toggle-table"

                try:
                    elemento_botao = driver.find_element(
                        By.CSS_SELECTOR, seletor_do_botao)
                    svg_dentro_do_botao = elemento_botao.find_element(
                        By.TAG_NAME, "svg")
                    estilo_svg = svg_dentro_do_botao.get_attribute(
                        "style") or ""

                    if "rotate(0deg)" in estilo_svg:  # fechado
                        log.info(
                            f" -> {nome_liga_completo} está FECHADA. Expandindo...")
                        driver.execute_script(
                            "arguments[0].click();", elemento_botao)

                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR,
                                     f"#{id_do_bloco} tbody[id^='xmatch_']")
                                )
                            )
                            log.info(
                                f" -> {nome_liga_completo} expandida com sucesso.")
                        except:
                            log.warning(
                                f" -> {nome_liga_completo} não expandiu a tempo.")
                    else:
                        log.info(f" -> {nome_liga_completo} já está ABERTA.")
                except Exception as e:
                    log.error(
                        f" -> Erro ao verificar/clicar {nome_liga_completo}: {e}")

            except Exception:
                continue

        # --- FASE B: EXTRAIR OS JOGOS ---
        log.info("Fase B: Extraindo jogos...")
        time.sleep(2)

        html_final = driver.page_source
        soup_final = BeautifulSoup(html_final, 'html.parser')

        blocos_de_liga_final = soup_final.find_all(
            'div', id=lambda x: x and x.startswith('league_'))

        for bloco in blocos_de_liga_final:
            try:
                pais_el = bloco.select_one("span.d-block.d-md-inline")
                pais = pais_el.text.strip() if pais_el else ''
                liga_el = bloco.select_one("span.font-bold")
                liga = liga_el.text.strip() if liga_el else ''
                nome_liga_completo = f"{pais} - {liga}" if pais else liga

                if nome_liga_completo.lower() not in ligas_normalizadas:
                    continue

                corpos_de_jogo = bloco.find_all(
                    'tbody', id=lambda x: x and x.startswith('xmatch_'))

                for corpo_jogo in corpos_de_jogo:
                    linha = corpo_jogo.find('tr')
                    if not linha:
                        continue

                    try:
                        id_jogo = corpo_jogo['id'].replace('xmatch_', '')
                        hora = linha.select('td')[1].text.strip()
                        time_casa = linha.select_one(
                            "td.text-md-right span.team").text.strip()
                        time_fora = linha.select('td')[4].select_one(
                            "span.team").text.strip()
                        link_confronto = "https://redscores.com" + \
                            linha.select_one("td.text-md-right a")['href']

                        odds_cells = linha.select("td")
                        odd_h = odds_cells[12].text.strip() if len(
                            odds_cells) > 12 and odds_cells[12].text.strip() != '-' else None
                        odd_d = odds_cells[13].text.strip() if len(
                            odds_cells) > 13 and odds_cells[13].text.strip() != '-' else None
                        odd_a = odds_cells[14].text.strip() if len(
                            odds_cells) > 14 and odds_cells[14].text.strip() != '-' else None

                        lista_de_jogos.append({
                            "id_jogo": id_jogo,
                            "liga": nome_liga_completo,
                            "hora": hora,
                            "home": time_casa,
                            "away": time_fora,
                            "link_confronto": link_confronto,
                            "odd_h": odd_h,
                            "odd_d": odd_d,
                            "odd_a": odd_a,
                        })
                    except Exception as e:
                        log.error(
                            f" -> Erro ao extrair jogo em {nome_liga_completo}: {e}")
                        continue
            except Exception:
                continue

    except Exception as e:
        log.error(f"Ocorreu um erro inesperado: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass

    return lista_de_jogos

# ==========================
# Obter links de equipes do confronto
# ==========================
def obter_links_equipes_confronto(url_confronto):
    """
    Visita a página de um confronto e extrai os links das páginas das duas equipes.
    Usa uma espera passiva (time.sleep) para evitar crashes.
    """
    driver = _iniciar_driver()
    if not driver:
        return None, None

    try:
        driver.get(url_confronto)
        print(f"-> A aguardar a página do confronto: {url_confronto}")
        time.sleep(8)

        html_content = driver.page_source
        if not html_content or "<body" not in html_content.lower():
            print(f"-> ERRO: Conteúdo vazio para {url_confronto}")
            return None, None

        soup = BeautifulSoup(html_content, 'html.parser')

        seletor_equipes = "div.match-detail__name a"
        links_equipes = soup.select(seletor_equipes)

        if len(links_equipes) >= 2:
            link_home = "https://redscores.com" + links_equipes[0]['href']
            link_away = "https://redscores.com" + links_equipes[1]['href']
            return link_home, link_away
        else:
            print(
                f"-> AVISO: Encontrados {len(links_equipes)} links de equipes em {url_confronto}. Esperava 2.")
            return None, None

    except Exception as e:
        print(f"-> ERRO ao obter links de equipes de {url_confronto}: {e}")
        return None, None
    finally:
        if driver:
            driver.quit()

# ==========================
# Obter dados dos times
# ==========================


def raspar_dados_time(time_url, liga_principal, jogos_existentes, ligas_permitidas_set, limite_jogos=cfg.LIMITE_JOGOS_POR_TIME):
    jogos_raspados = []
    driver = _iniciar_driver()
    if not driver:
        return jogos_raspados

    try:
        driver.get(time_url)
        while True:
            try:
                jogos_atuais = driver.find_elements(
                    By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")
                if len(jogos_atuais) >= limite_jogos:
                    break

                see_more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "link-see-more")))
                driver.execute_script("arguments[0].click();", see_more_button)
                WebDriverWait(driver, 5).until(lambda d: len(d.find_elements(
                    By.CSS_SELECTOR, "div.match-grid__bottom tbody tr")) > len(jogos_atuais))
            except Exception:
                break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        match_grid = soup.find('div', class_='match-grid__bottom')

        if match_grid:
            for linha in match_grid.select('tbody tr'):
                try:
                    celulas = linha.find_all('td')
                    if len(celulas) > 10:
                        liga_img = celulas[1].find('img')
                        liga_local = liga_img['alt'].strip(
                        ) if liga_img else ''
                        if not liga_local:
                            continue

                        liga_final_para_salvar = None

                        # 1. Verifica se a liga local pertence à liga principal da equipa
                        if liga_local.lower() in liga_principal.lower():
                            liga_final_para_salvar = liga_principal
                        else:
                            # 2. Se não, verifica se a liga local (ex: Copa Libertadores) está na lista de permitidas
                            for liga_permitida in ligas_permitidas_set:
                                if liga_local.lower() in liga_permitida.lower():
                                    liga_final_para_salvar = liga_permitida
                                    break

                        # 3. Se a liga do jogo não for encontrada nas nossas regras, pula para a próxima linha
                        if not liga_final_para_salvar:
                            continue

                        data = celulas[0].text.strip()
                        time_casa = celulas[2].text.strip()
                        time_fora = celulas[4].text.strip()

                        data_padronizada = _formatar_data(data)
                        home_normalizado = " ".join(time_casa.split()).title()
                        away_normalizado = " ".join(time_fora.split()).title()
                        identificador_jogo_atual = (
                            data_padronizada, home_normalizado, away_normalizado)

                        if identificador_jogo_atual in jogos_existentes:
                            break

                        placar_texto = celulas[3].text.strip()
                        placar_ht = celulas[5].text.strip()
                        chutes = celulas[6].text.strip()
                        chutes_gol = celulas[7].text.strip()
                        ataques = celulas[8].text.strip()
                        escanteios = celulas[9].text.strip()
                        odd_h = celulas[11].text.strip()
                        odd_d = celulas[12].text.strip()
                        odd_a = celulas[13].text.strip()

                        jogos_raspados.append({
                            "Liga": liga_final_para_salvar, "Data": data, "Home": time_casa, "Away": time_fora,
                            "Placar_FT": placar_texto, "Placar_HT": placar_ht, "Chutes": chutes,
                            "Chutes_Gol": chutes_gol, "Ataques": ataques, "Escanteios": escanteios,
                            "Odd_H_str": odd_h, "Odd_D_str": odd_d, "Odd_A_str": odd_a
                        })
                except Exception:
                    continue
    except Exception as e:
        print(f"Ocorreu um erro geral com o Selenium em {time_url}: {e}")
    finally:
        if driver:
            driver.quit()

    return jogos_raspados
 
# ==========================
# Processamento dos dados
# ==========================
def processar_dados_raspados(lista_de_jogos):
    jogos_processados = []
    for jogo in lista_de_jogos:
        try:
            data_padronizada = _formatar_data(jogo['Data'])
            if data_padronizada is None:
                continue

            liga = " ".join(jogo['Liga'].split()).title()
            placar_ft = _converter_stat_para_int(jogo['Placar_FT'])
            placar_ht = _converter_stat_para_int(jogo['Placar_HT'])
            chutes = _converter_stat_para_int(jogo['Chutes'])
            chutes_gol = _converter_stat_para_int(jogo['Chutes_Gol'])
            ataques = _converter_stat_para_int(jogo['Ataques'])
            escanteios = _converter_stat_para_int(jogo['Escanteios'])

            odd_h = float(
                jogo['Odd_H_str']) if jogo['Odd_H_str'] and jogo['Odd_H_str'] != '-' else 0.0
            odd_d = float(
                jogo['Odd_D_str']) if jogo['Odd_D_str'] and jogo['Odd_D_str'] != '-' else 0.0
            odd_a = float(
                jogo['Odd_A_str']) if jogo['Odd_A_str'] and jogo['Odd_A_str'] != '-' else 0.0

            home = " ".join(jogo['Home'].split())
            away = " ".join(jogo['Away'].split())

            jogos_processados.append({
                "Liga": liga, "Data": data_padronizada, "Home": home, "Away": away,
                "H_Gols_FT": placar_ft[0], "A_Gols_FT": placar_ft[1],
                "H_Gols_HT": placar_ht[0], "A_Gols_HT": placar_ht[1],
                "H_Chute": chutes[0], "A_Chute": chutes[1],
                "H_Chute_Gol": chutes_gol[0], "A_Chute_Gol": chutes_gol[1],
                "H_Ataques": ataques[0], "A_Ataques": ataques[1],
                "H_Escanteios": escanteios[0], "A_Escanteios": escanteios[1],
                "Odd_H": odd_h, "Odd_D": odd_d, "Odd_A": odd_a
            })
        except Exception as e:
            print(f"[DIAGNÓSTICO] ERRO ao processar o jogo acima: {e}")
            continue
    return pd.DataFrame(jogos_processados)