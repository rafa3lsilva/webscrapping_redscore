import pandas as pd
from bs4 import BeautifulSoup
import ligas_config as cfg
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

# Configura logger específico do módulo
log = logging.getLogger("coletor")
log.setLevel(logging.INFO)
if not log.handlers:
    handler = logging.FileHandler("coletor.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)


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
def raspar_jogos_de_amanha(driver, ligas_permitidas_set):
    """
    Raspagem de jogos - versão produtiva:
    - Apenas ligas abertas
    - Logging completo
    - Espera dinâmica inicial para garantir HTML carregado
    """
    lista_de_jogos = []
    #driver = _iniciar_driver()
    if not driver:
        log.error("Falha ao iniciar o driver")
        return lista_de_jogos

    try:
        driver.get("https://redscores.com/pt-br/futebol/amanha")
        log.info("Página de jogos de amanhã aberta. Verificando banner de cookies...")

        # Aceitar cookies se existir
        try:
            botao_aceitar = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "div.cookieinfo-close"))
            )
            botao_aceitar.click()
            log.info("Banner de cookies aceito.")
        except Exception:
            log.info("Nenhum banner de cookies encontrado. Continuando...")

        # Espera para garantir que o HTML principal carregou
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[id^='league_']"))
        )
        time.sleep(1)  # Pequeno buffer para estabilidade

        ligas_normalizadas = {l.lower() for l in ligas_permitidas_set}

        # Extrai HTML final
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        blocos_de_liga = soup.find_all(
            'div', id=lambda x: x and x.startswith('league_'))

        for bloco in blocos_de_liga:
            pais_el = bloco.select_one("span.d-block.d-md-inline")
            pais = pais_el.text.strip() if pais_el else ''
            liga_el = bloco.select_one("span.font-bold")
            liga = liga_el.text.strip() if liga_el else ''
            nome_liga_completo = f"{pais} - {liga}" if pais else liga

            if nome_liga_completo.lower() not in ligas_normalizadas:
                continue

            # Verifica se a liga está aberta (tbody com jogos presente)
            corpos_de_jogo = bloco.find_all(
                'tbody', id=lambda x: x and x.startswith('xmatch_'))
            if not corpos_de_jogo:
                log.info(f" -> {nome_liga_completo} está fechada. Ignorando.")
                continue
            else:
                log.info(
                    f" -> {nome_liga_completo} está aberta. Processando...")

            for corpo_jogo in corpos_de_jogo:
                linha = corpo_jogo.find('tr')
                if not linha:
                    continue
                try:
                    id_jogo = corpo_jogo['id'].replace('xmatch_', '')
                    tds = linha.find_all('td')
                    hora = tds[1].text.strip()
                    time_casa = tds[2].select_one("span.team").text.strip()
                    time_fora = tds[4].select_one("span.team").text.strip()
                    link_confronto = "https://redscores.com" + \
                        tds[2].select_one("a")['href']

                    odd_h = tds[12].text.strip() if len(
                        tds) > 12 and tds[12].text.strip() != '-' else None
                    odd_d = tds[13].text.strip() if len(
                        tds) > 13 and tds[13].text.strip() != '-' else None
                    odd_a = tds[14].text.strip() if len(
                        tds) > 14 and tds[14].text.strip() != '-' else None

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

    except Exception as e:
        log.error(f"Ocorreu um erro inesperado: {e}")
    log.info(
        f"Raspagem finalizada. Total de jogos coletados: {len(lista_de_jogos)}")
    return lista_de_jogos

# ==========================
# Obter links de equipes do confronto
# ==========================
def obter_links_equipes_confronto(driver, url_confronto):
    """
    Visita a página de um confronto e extrai os links das páginas das duas equipes.
    Usa uma espera passiva (time.sleep) para evitar crashes.
    """
    if not driver:
        return None, None

    try:
        driver.get(url_confronto)
        # Otimização: Substituir time.sleep por WebDriverWait
        log.info(f"-> A aguardar a página do confronto: {url_confronto}")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.match-detail__name a"))
        )
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

# ==========================
# Obter dados dos times
# ==========================
def raspar_dados_time(driver, time_url, liga_principal, jogos_existentes, ligas_permitidas_set, limite_jogos=cfg.LIMITE_JOGOS_POR_TIME):
    jogos_raspados = []
    if not driver:
        return jogos_raspados

    try:
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