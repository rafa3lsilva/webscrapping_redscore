import pandas as pd
from bs4 import BeautifulSoup
import ligas_config as cfg
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import csv

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
def _formatar_data(texto_data: str):
    """
    Converte datas do site (formato MM-DD-YY ou MM/DD/YY)
    para o formato padronizado YYYY-MM-DD.
    """
    if not texto_data or not isinstance(texto_data, str):
        return None
    try:
        # Forçar interpretação como formato americano (MM-DD-YY)
        data_obj = pd.to_datetime(texto_data, format="%m/%d/%y", errors="coerce")
        if pd.isna(data_obj):
            # fallback: tenta parse genérico
            data_obj = pd.to_datetime(texto_data, errors="coerce")
        if pd.isna(data_obj):
            return None
        return data_obj.strftime("%Y-%m-%d")
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
    Raspagem da agenda de amanhã (robusta).
    Retorna lista de dicts com chaves:
      id_jogo, liga, hora, home, away, link_confronto, odd_h, odd_d, odd_a
    """
    jogos = []
    total_encontrados = 0
    total_validos = 0
    total_incompletos = 0
    total_filtrados = 0

    # normaliza ligas permitidas para comparação case-insensitive
    ligas_normalizadas = {l.strip().lower() for l in ligas_permitidas_set if l}

    try:
        driver.get("https://redscores.com/pt-br/futebol/amanha")
        log.info("[AGENDA] Acessando /futebol/amanha")

        # espera genérica por qualquer estrutura plausível da página
        try:
            WebDriverWait(driver, 12).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div[id^='league_'], div.fixtures, table"))
            )
        except Exception:
            log.warning(
                "[AGENDA] Elemento esperado não apareceu rapidamente — seguindo com o HTML atual")
            time.sleep(1)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # 1) Primeiro: tenta estrutura antiga por blocos de liga (id league_)
        blocos_de_liga = soup.find_all(
            'div', id=lambda x: x and x.startswith('league_'))
        if blocos_de_liga:
            log.info(
                f"[AGENDA] Detectados {len(blocos_de_liga)} blocos de liga (layout 'league_').")
            for bloco in blocos_de_liga:
                pais_el = bloco.select_one("span.d-block.d-md-inline")
                pais = pais_el.text.strip() if pais_el else ''
                liga_el = bloco.select_one("span.font-bold")
                liga = liga_el.text.strip() if liga_el else ''
                nome_liga_completo = f"{pais} - {liga}" if pais else liga
                if nome_liga_completo and nome_liga_completo.lower() not in ligas_normalizadas:
                    # pular ligas não permitidas
                    continue

                corpos_de_jogo = bloco.find_all(
                    'tbody', id=lambda x: x and x.startswith('xmatch_'))
                for corpo_jogo in corpos_de_jogo:
                    try:
                        linha = corpo_jogo.find('tr')
                        if not linha:
                            continue
                        id_jogo = corpo_jogo.get('id', '').replace(
                            'xmatch_', '') or None
                        tds = linha.find_all('td')

                        # tentativa segura de extrair campos (com validações)
                        hora = tds[1].get_text(
                            strip=True) if len(tds) > 1 else None
                        # tenta extrair nomes dos times com diferentes possíveis seletores

                        def extrair_time_from_td(td_idx):
                            if len(tds) > td_idx:
                                el = tds[td_idx]
                                # prefer span.team, fallback a texto direto
                                s = el.select_one("span.team")
                                if s and s.get_text(strip=True):
                                    return s.get_text(strip=True)
                                # fallback: link ou texto
                                a = el.select_one("a")
                                if a and a.get_text(strip=True):
                                    return a.get_text(strip=True)
                                return el.get_text(strip=True)
                            return None

                        time_casa = extrair_time_from_td(2)
                        time_fora = extrair_time_from_td(4)
                        link_elem = tds[2].select_one(
                            "a") if len(tds) > 2 else None
                        link_confronto = "https://redscores.com" + \
                            link_elem['href'] if link_elem and link_elem.has_attr(
                                'href') else None

                        if not hora or not time_casa or not time_fora or not link_confronto:
                            total_incompletos += 1
                            log.warning(
                                f"[AGENDA] Jogo incompleto (league_): LIGA={nome_liga_completo}, ID={id_jogo}, HORA={hora}, TIMES={[time_casa, time_fora]}, LINK={link_confronto}")
                            with open("jogos_agenda_incompletos.csv", "a", newline="", encoding="utf-8") as f:
                                writer = csv.writer(f)
                                writer.writerow(
                                    [nome_liga_completo, id_jogo, hora, time_casa, time_fora, link_confronto])
                            continue

                        jogos.append({
                            "id_jogo": id_jogo,
                            "liga": nome_liga_completo,
                            "hora": hora,
                            "home": " ".join(time_casa.split()),
                            "away": " ".join(time_fora.split()),
                            "link_confronto": link_confronto,
                            "odd_h": None, "odd_d": None, "odd_a": None
                        })
                        total_validos += 1
                    except Exception as e:
                        total_incompletos += 1
                        log.error(
                            f"[AGENDA] Erro ao processar jogo (league_): {e}")
                        with open("jogos_agenda_incompletos.csv", "a", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(["ERRO_LEAGUE", str(e)])
                        continue

        else:
            # 2) Fallback: tenta layout moderno (.fixtures__item / fixtures)
            jogos_html = soup.select(
                "div.fixtures__item, li.fixture-item, div.fixture, div.match-row")
            total_encontrados = len(jogos_html)
            log.info(
                f"[AGENDA] Layout 'fixtures' detectado: itens encontrados = {total_encontrados}")

            for item in jogos_html:
                try:
                    # liga
                    liga_el = item.select_one(
                        "div.fixtures__tournament, .tournament-name, .competition")
                    nome_liga = liga_el.get_text(
                        strip=True) if liga_el else None
                    if nome_liga and nome_liga.lower() not in ligas_normalizadas:
                        total_filtrados += 1
                        continue

                    # hora
                    hora_el = item.select_one(
                        "div.fixtures__time, span.fixture__time, .time")
                    hora_texto = hora_el.get_text(
                        strip=True) if hora_el else None

                    # times (varia bastante)
                    equipes = item.select(
                        "div.fixtures__name, span.team-name, .team .name, .team")
                    times = [e.get_text(strip=True)
                             for e in equipes if e.get_text(strip=True)]
                    # se ainda não tiver 2, tenta pegar por spans separados
                    if len(times) < 2:
                        left = item.select_one(
                            ".team--home, .home .name, .team-left")
                        right = item.select_one(
                            ".team--away, .away .name, .team-right")
                        if left:
                            tl = left.get_text(strip=True)
                        else:
                            tl = None
                        if right:
                            tr = right.get_text(strip=True)
                        else:
                            tr = None
                        times = [t for t in [tl, tr] if t]

                    # link
                    link_tag = item.select_one(
                        "a[href*='/match/'], a.fixtures__match, a.match-link")
                    link_url = (
                        "https://redscores.com" + link_tag['href']) if link_tag and link_tag.has_attr('href') else None

                    if not hora_texto or len(times) < 2 or not link_url:
                        total_incompletos += 1
                        log.warning(
                            f"[AGENDA] Jogo incompleto (fixtures): LIGA={nome_liga}, HORA={hora_texto}, TIMES={times}, LINK={link_url}")
                        with open("jogos_agenda_incompletos.csv", "a", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(
                                [nome_liga, hora_texto, ";".join(times), link_url])
                        continue

                    jogos.append({
                        "id_jogo": None,
                        "liga": nome_liga,
                        "hora": hora_texto,
                        "home": " ".join(times[0].split()),
                        "away": " ".join(times[1].split()),
                        "link_confronto": link_url,
                        "odd_h": None, "odd_d": None, "odd_a": None
                    })
                    total_validos += 1

                except Exception as e:
                    total_incompletos += 1
                    log.error(
                        f"[AGENDA] Erro ao processar item (fixtures): {e}")
                    with open("jogos_agenda_incompletos.csv", "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["ERRO_FIXTURES", str(e)])
                    continue

        # Se não encontrou nada por nenhum método, loga para diagnóstico
        if total_validos == 0:
            log.error(
                "[AGENDA] Nenhum jogo válido coletado. Verifique seletores e HTML da página.")
            # salva snapshot do HTML para debug
            with open("pagina_amanha_snapshot.html", "w", encoding="utf-8") as f:
                f.write(html)

        log.info(
            f"[AGENDA] Finalizado. Válidos: {total_validos}, Incompletos: {total_incompletos}, Filtrados: {total_filtrados}")
        return jogos

    except Exception as e:
        log.error(f"[AGENDA] Falha geral na raspagem: {e}")
        return []


# ==========================
# Obter links de equipes do confronto
# ==========================
def obter_links_equipes_confronto(driver, url_confronto):
    """
    Visita a página de um confronto e extrai os links das páginas das duas equipes.
    - Usa WebDriverWait para aguardar carregamento.
    - Faz log detalhado.
    - Salva casos problemáticos em CSV para auditoria.
    - Tenta fallback para capturar nomes mesmo que links não estejam presentes.
    """
    if not driver:
        log.error(
            f"[DRIVER] Driver não inicializado ao tentar abrir {url_confronto}")
        return None, None

    try:
        driver.get(url_confronto)
        log.info(
            f"[CONFRONTO] Aguardando carregamento da página: {url_confronto}")

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.match-detail__teams"))
        )
        html_content = driver.page_source
        if not html_content or "<body" not in html_content.lower():
            log.error(f"[HTML] Página vazia para {url_confronto}")
            return None, None

        soup = BeautifulSoup(html_content, "html.parser")

        # Seletor mais robusto - pega links mesmo que a classe varie
        links_equipes = soup.select(
            "div.match-detail__teams a, div.match-detail__name a, div.match-detail__team a")

        if len(links_equipes) >= 2:
            link_home = "https://redscores.com" + links_equipes[0]["href"]
            link_away = "https://redscores.com" + links_equipes[1]["href"]
            log.info(
                f"[CONFRONTO] Links encontrados: HOME={link_home}, AWAY={link_away}")
            return link_home, link_away
        else:
            # Fallback para capturar nomes mesmo sem link
            nomes_equipes = [t.get_text(strip=True)
                             for t in soup.select("div.match-detail__name")]
            log.warning(
                f"[CONFRONTO] Não encontrou 2 links para {url_confronto}. Encontrados: {len(links_equipes)}. Nomes detectados: {nomes_equipes}")

            # Salva em CSV para análise futura
            with open("jogos_incompletos.csv", "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [url_confronto, len(links_equipes), ";".join(nomes_equipes)])

            return None, None

    except Exception as e:
        log.error(f"[ERRO] Falha ao processar {url_confronto}: {e}")
        with open("jogos_incompletos.csv", "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([url_confronto, "ERRO", str(e)])
        return None, None

# ==========================
# Obter dados dos times
# ==========================
def raspar_dados_time(driver, time_url, liga_principal, jogos_existentes, ligas_permitidas_set, limite_jogos=cfg.LIMITE_JOGOS_POR_TIME):
    jogos_raspados = []
    if not driver:
        return jogos_raspados
    # Abrir a página do time antes de buscar elementos
    try:
        driver.get(time_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.match-grid__bottom"))
        )
    except Exception:
        # se não conseguiu carregar, segue tentando com o estado atual do driver
        pass

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