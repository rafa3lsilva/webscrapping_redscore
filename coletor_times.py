import sqlite3
import time
import logging
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from auth_redscore import REDSCORE_USER, REDSCORE_PASS
from login_redscore import login_redscore

# Configuração de Logs
logging.basicConfig(
    filename='coletor_times.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger("bottom_up")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
log.addHandler(console)

DB_NAME = "dados_historicos_novo.db"

def inicializar_banco():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jogos_detalhados (
            Link TEXT PRIMARY KEY,
            Data TEXT, Hora TEXT, Liga TEXT, Rodada TEXT,
            Home TEXT, Away TEXT,
            Placar_FT TEXT, Placar_HT TEXT,
            H_Posse REAL, A_Posse REAL,
            H_xG REAL, A_xG REAL,
            H_Chutes INTEGER, A_Chutes INTEGER,
            H_Chutes_Alvo INTEGER, A_Chutes_Alvo INTEGER,
            H_Escanteios INTEGER, A_Escanteios INTEGER,
            H_Faltas INTEGER, A_Faltas INTEGER,
            H_Amarelos INTEGER, A_Amarelos INTEGER,
            H_Vermelhos INTEGER, A_Vermelhos INTEGER,
            H_Ataques INTEGER, A_Ataques INTEGER,
            H_Ataques_Perigosos INTEGER, A_Ataques_Perigosos INTEGER,
            H_Defesas_Goleiro INTEGER, A_Defesas_Goleiro INTEGER,
            H_Passes INTEGER, A_Passes INTEGER,
            H_Acerto_Passe REAL, A_Acerto_Passe REAL,
            H_Passes_Decisivos INTEGER, A_Passes_Decisivos INTEGER,
            H_Cruzamentos INTEGER, A_Cruzamentos INTEGER,
            H_Dribles INTEGER, A_Dribles INTEGER,
            
            -- 1º Tempo
            H1_Posse REAL, A1_Posse REAL,
            H1_Chutes INTEGER, A1_Chutes INTEGER, H1_Chutes_Alvo INTEGER, A1_Chutes_Alvo INTEGER,
            H1_Escanteios INTEGER, A1_Escanteios INTEGER,
            H1_Ataques INTEGER, A1_Ataques INTEGER, H1_Ataques_Perigosos INTEGER, A1_Ataques_Perigosos INTEGER,
            
            -- 2º Tempo
            H2_Posse REAL, A2_Posse REAL,
            H2_Chutes INTEGER, A2_Chutes INTEGER, H2_Chutes_Alvo INTEGER, A2_Chutes_Alvo INTEGER,
            H2_Escanteios INTEGER, A2_Escanteios INTEGER,
            H2_Ataques INTEGER, A2_Ataques INTEGER, H2_Ataques_Perigosos INTEGER, A2_Ataques_Perigosos INTEGER,

            Odd_H_Open REAL, Odd_D_Open REAL, Odd_A_Open REAL,
            Odd_H_Close REAL, Odd_D_Close REAL, Odd_A_Close REAL
        )""")
        conn.commit()

def fase1_obter_times(driver, url_liga):
    log.info(f"[FASE 1] Acessando liga: {url_liga}")
    driver.get(url_liga)
    wait = WebDriverWait(driver, 10)
    time.sleep(5) # Esperar a tabela de classificação carregar
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    teams = set()
    for a in soup.select('table.table-data__table a[href*="/pt-br/team/"]'):
        href = a['href'].split('#')[0]
        if not href.startswith("http"):
            href = "https://redscores.com" + href
        teams.add(href)
        
    log.info(f"[FASE 1] Concluída. {len(teams)} times encontrados na liga.")
    return list(teams)

def fase2_obter_links_partidas(driver, urls_times, liga_nome):
    log.info(f"[FASE 2] Iniciando mineração de partidas para {len(urls_times)} times.")
    links_partidas = set()
    wait = WebDriverWait(driver, 10)
    
    for idx, team_url in enumerate(urls_times, 1):
        log.info(f"  -> Explorando Time {idx}/{len(urls_times)}: {team_url}")
        driver.get(team_url)
        
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.match-grid__bottom")))
        except:
            log.warning(f"  -> Tabela não carregou para {team_url}")
            continue
            
        clicks = 0
        while True:
            try:
                # Procura o botão 'Mostrar mais jogos' ou similar
                btn = driver.find_element(By.XPATH, "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'mostrar mais') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ver mais') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'more')] | //button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'mostrar mais') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ver mais') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'more')]")
                if btn.is_displayed():
                    driver.execute_script("arguments[0].click();", btn)
                    clicks += 1
                    time.sleep(2)
                else:
                    break
            except:
                break
                
        log.info(f"  -> Expandido {clicks} vezes.")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Filtra os links pela liga desejada
        encontrados_time = 0
        for tr in soup.select("div.match-grid__bottom tbody tr"):
            tds = tr.find_all('td')
            if len(tds) < 2:
                continue
                
            img = tds[1].find('img')
            liga_local = img['alt'].strip() if img and 'alt' in img.attrs else ""
            
            if liga_nome.lower() in liga_local.lower():
                a_tag = tr.select_one("a[href*='/match/']")
                if a_tag:
                    href = a_tag['href'].split('#')[0]
                    if not href.startswith("http"):
                        href = "https://redscores.com" + href
                    links_partidas.add(href)
                    encontrados_time += 1
                    
        log.info(f"  -> {encontrados_time} partidas VÁLIDAS encontradas na tela deste time.")
        
    log.info(f"[FASE 2] Concluída. {len(links_partidas)} partidas ÚNICAS encontradas no total.")
    return list(links_partidas)

def fase3_raspar_detalhes(driver, urls_partidas, nome_liga_esperada):
    log.info(f"[FASE 3] Iniciando Deep Scraping de {len(urls_partidas)} partidas...")
    
    with sqlite3.connect(DB_NAME) as conn:
        for idx, url in enumerate(urls_partidas, 1):
            log.info(f"  -> Coletando Partida {idx}/{len(urls_partidas)}: {url}")
            driver.get(url)
            time.sleep(2)
            
            # Scroll para carregar estatísticas
            driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(1)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # --- FILTRO DE LIGA (BREADCRUMBS) ---
            breadcrumbs = soup.select('li.breadcrumb__item a')
            if len(breadcrumbs) >= 3:
                liga_real = breadcrumbs[2].get_text(strip=True)
                if nome_liga_esperada.lower() not in liga_real.lower():
                    log.info(f"  -> Pulando jogo de outra liga/copa: {liga_real}")
                    continue

            # --- METADADOS ---
            # Exemplo cabeçalho: 04/26/26 18:30 - 13.Rodada
            data_str = ""
            hora_str = ""
            rodada = ""
            header_elem = soup.select_one("header.match-detail__header")
            if header_elem:
                p_tag = header_elem.find('p')
                if p_tag:
                    txt = p_tag.get_text(" ", strip=True)
                    if '-' in txt:
                        data_hora, rodada = [x.strip() for x in txt.split('-', 1)]
                        partes = data_hora.split(' ')
                        data_str = partes[0] if len(partes) > 0 else ""
                        hora_str = partes[1] if len(partes) > 1 else ""
                    else:
                        partes = txt.split(' ')
                        data_str = partes[0] if len(partes) > 0 else ""
                        hora_str = partes[1] if len(partes) > 1 else ""
            
            # Extraindo nomes dos times do Header
            nomes_a = soup.select("div.match-detail__name a")
            home = nomes_a[0].get_text(strip=True) if len(nomes_a) > 0 else ""
            away = nomes_a[1].get_text(strip=True) if len(nomes_a) > 1 else ""
            
            # Status e Placar (Filtro Finalizado)
            status_text = ""
            status_elem = soup.select_one("p.match-detail__status")
            if status_elem:
                status_text = status_elem.get_text(strip=True)
            
            ft_div = soup.select_one("div.match-detail__score")
            placar_ft = ft_div.get_text(strip=True) if ft_div else ""
            
            # Se não for MATCH REPORT ou FIM DE JOGO, e tiver ":" ou for jogo futuro, pula
            if "MATCH REPORT" not in status_text and "FIM" not in status_text.upper():
                 if ':' in placar_ft or '-' not in placar_ft:
                    log.info(f"  -> Jogo não finalizado ignorado: Status='{status_text}', Placar='{placar_ft}'")
                    continue
            
            ht_div = soup.find(lambda tag: tag.name == "td" and "half-time" in tag.get("class", []) and "FT" in tag.text)
            placar_ht = ht_div.get_text(strip=True).replace(" FT", "") if ht_div else ""

            # --- ESTATÍSTICAS ---
            stats_ft = {}
            stats_1h = {}
            stats_2h = {}
            
            def parse_stats_to_dict(s):
                d = {}
                for title_elem in s.select('.progress-title, .stats-ranking-item'):
                    texto_titulo = title_elem.get_text(" ", strip=True).split('\n')[0].strip()
                    container = title_elem.find_next_sibling('.progress-container')
                    if container:
                        v_left = container.select_one('.progress-value-left')
                        v_right = container.select_one('.progress-value-right')
                        d[texto_titulo] = (v_left.get_text(strip=True) if v_left else "0", v_right.get_text(strip=True) if v_right else "0")
                        continue
                    rail = title_elem.select_one('.progress-bar__rail')
                    if rail:
                        v_left = rail.select_one('.progress-bar__value_left')
                        v_right = rail.select_one('.progress-bar__value_right')
                        val_l = v_left.get_text(" ", strip=True).split('(')[0].strip().replace('%', '') if v_left else "0"
                        val_r = v_right.get_text(" ", strip=True).split('(')[0].strip().replace('%', '') if v_right else "0"
                        d[texto_titulo] = (val_l, val_r)
                return d

            def get_val(d, key, is_home, default="0"):
                for k, vals in d.items():
                    if key.lower() in k.lower():
                        v = vals[0] if is_home else vals[1]
                        return float(str(v).replace('%', ''))
                return float(default)

            def get_val_split(d, key, is_home):
                for k, vals in d.items():
                    if key.lower() in k.lower():
                        v = vals[0] if is_home else vals[1]
                        return int(v.split('/')[0]) if '/' in str(v) else int(v)
                return 0

            # Coleção por período (Jogo, 1T, 2T)
            periods = [("ft", stats_ft), ("ht", stats_1h), ("sh", stats_2h)]
            for x_val, d_target in periods:
                try:
                    btn = driver.find_element(By.XPATH, f"//a[@xfcg-value='{x_val}']")
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(1)
                    
                    if x_val == "ft":
                        try:
                            def_btn = driver.find_element(By.XPATH, "//a[contains(text(), 'Defesa')]")
                            driver.execute_script("arguments[0].click();", def_btn)
                            time.sleep(0.5)
                        except: pass

                    current_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    extracted = parse_stats_to_dict(current_soup)
                    d_target.update(extracted)
                    
                    if x_val == "ft":
                        cards_containers = current_soup.select("div.col-4.text-center")
                        for cc in cards_containers:
                            if "Cartões" in cc.text:
                                val_elem = cc.select_one(".cards-counter__number")
                                val = val_elem.get_text(strip=True) if val_elem else "0"
                                if "H_Cartoes" not in d_target: d_target["H_Cartoes"] = val
                                else: d_target["A_Cartoes"] = val
                except:
                    pass

            # --- MAPEAMENTO FINAL ---
            h_posse, a_posse = get_val(stats_ft, "Posse", True), get_val(stats_ft, "Posse", False)
            h_xg, a_xg = get_val(stats_ft, "xG", True), get_val(stats_ft, "xG", False)
            h_chutes, a_chutes = int(get_val(stats_ft, "Total de chutes", True)), int(get_val(stats_ft, "Total de chutes", False))
            h_chutes_alvo, a_chutes_alvo = int(get_val(stats_ft, "Chutes a gol", True)), int(get_val(stats_ft, "Chutes a gol", False))
            h_escanteios, a_escanteios = int(get_val(stats_ft, "Escanteios", True)), int(get_val(stats_ft, "Escanteios", False))
            h_faltas, a_faltas = int(get_val(stats_ft, "Faltas", True)), int(get_val(stats_ft, "Faltas", False))
            h_amarelos = int(stats_ft.get("H_Cartoes", 0))
            a_amarelos = int(stats_ft.get("A_Cartoes", 0))
            h_ataques, a_ataques = int(get_val(stats_ft, "Ataques", True)), int(get_val(stats_ft, "Ataques", False))
            h_ataques_p, a_ataques_p = int(get_val(stats_ft, "Ataques Perigosos", True)), int(get_val(stats_ft, "Ataques Perigosos", False))
            h_defesas, a_defesas = int(get_val(stats_ft, "Defesas do goleiro", True)), int(get_val(stats_ft, "Defesas do goleiro", False))
            h_passes, a_passes = int(get_val(stats_ft, "Total de passes", True)), int(get_val(stats_ft, "Total de passes", False))
            h_acerto_passe, a_acerto_passe = get_val(stats_ft, "Acerto no passe", True), get_val(stats_ft, "Acerto no passe", False)
            h_passes_decisivos, a_passes_decisivos = int(get_val(stats_ft, "Passes decisivos", True)), int(get_val(stats_ft, "Passes decisivos", False))
            h_cruzamentos, a_cruzamentos = get_val_split(stats_ft, "Cruzamentos", True), get_val_split(stats_ft, "Cruzamentos", False)
            h_dribles, a_dribles = get_val_split(stats_ft, "Dribles", True), get_val_split(stats_ft, "Dribles", False)

            h1_posse, a1_posse = get_val(stats_1h, "Posse", True), get_val(stats_1h, "Posse", False)
            h1_chutes, a1_chutes = int(get_val(stats_1h, "Total de chutes", True)), int(get_val(stats_1h, "Total de chutes", False))
            h1_chutes_alvo, a1_chutes_alvo = int(get_val(stats_1h, "Chutes a gol", True)), int(get_val(stats_1h, "Chutes a gol", False))
            h1_escanteios, a1_escanteios = int(get_val(stats_1h, "Escanteios", True)), int(get_val(stats_1h, "Escanteios", False))
            h1_ataques, a1_ataques = int(get_val(stats_1h, "Ataques", True)), int(get_val(stats_1h, "Ataques", False))
            h1_ataques_p, a1_ataques_p = int(get_val(stats_1h, "Ataques Perigosos", True)), int(get_val(stats_1h, "Ataques Perigosos", False))

            h2_posse, a2_posse = get_val(stats_2h, "Posse", True), get_val(stats_2h, "Posse", False)
            h2_chutes, a2_chutes = int(get_val(stats_2h, "Total de chutes", True)), int(get_val(stats_2h, "Total de chutes", False))
            h2_chutes_alvo, a2_chutes_alvo = int(get_val(stats_2h, "Chutes a gol", True)), int(get_val(stats_2h, "Chutes a gol", False))
            h2_escanteios, a2_escanteios = int(get_val(stats_2h, "Escanteios", True)), int(get_val(stats_2h, "Escanteios", False))
            h2_ataques, a2_ataques = int(get_val(stats_2h, "Ataques", True)), int(get_val(stats_2h, "Ataques", False))
            h2_ataques_p, a2_ataques_p = int(get_val(stats_2h, "Ataques Perigosos", True)), int(get_val(stats_2h, "Ataques Perigosos", False))

            # --- ODDS ---
            odd_h_open = odd_d_open = odd_a_open = 0.0
            odd_h_close = odd_d_close = odd_a_close = 0.0
            for tr in soup.select("table.table-data__table--odds tbody tr"):
                th = tr.find('th')
                if not th: continue
                txt_th = th.get_text(strip=True).lower()
                tds = tr.find_all('td')
                if len(tds) >= 3:
                    try:
                        v1, vx, v2 = [float(t.get_text(strip=True).replace('↑', '').replace('↓', '')) for t in tds[:3]]
                        if "abertura" in txt_th: odd_h_open, odd_d_open, odd_a_open = v1, vx, v2
                        elif "antes" in txt_th: odd_h_close, odd_d_close, odd_a_close = v1, vx, v2
                    except: pass

            try:
                conn.execute("""
                INSERT OR IGNORE INTO jogos_detalhados 
                (Link, Data, Hora, Liga, Rodada, Home, Away, Placar_FT, Placar_HT,
                 H_Posse, A_Posse, H_xG, A_xG, H_Chutes, A_Chutes, H_Chutes_Alvo, A_Chutes_Alvo,
                 H_Escanteios, A_Escanteios, H_Faltas, A_Faltas, H_Amarelos, A_Amarelos,
                 H_Vermelhos, A_Vermelhos, H_Ataques, A_Ataques, H_Ataques_Perigosos, A_Ataques_Perigosos,
                 H_Defesas_Goleiro, A_Defesas_Goleiro, 
                 H_Passes, A_Passes, H_Acerto_Passe, A_Acerto_Passe,
                 H_Passes_Decisivos, A_Passes_Decisivos, H_Cruzamentos, A_Cruzamentos,
                 H_Dribles, A_Dribles,
                 H1_Posse, A1_Posse, H1_Chutes, A1_Chutes, H1_Chutes_Alvo, A1_Chutes_Alvo,
                 H1_Escanteios, A1_Escanteios, H1_Ataques, A1_Ataques, H1_Ataques_Perigosos, A1_Ataques_Perigosos,
                 H2_Posse, A2_Posse, H2_Chutes, A2_Chutes, H2_Chutes_Alvo, A2_Chutes_Alvo,
                 H2_Escanteios, A2_Escanteios, H2_Ataques, A2_Ataques, H2_Ataques_Perigosos, A2_Ataques_Perigosos,
                 Odd_H_Open, Odd_D_Open, Odd_A_Open, Odd_H_Close, Odd_D_Close, Odd_A_Close)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?, ?,?, ?,?,?,?, ?,?, ?,?,?,?, ?,?,?,?, ?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    url, data_str, hora_str, nome_liga_esperada, rodada, home, away, placar_ft, placar_ht,
                    h_posse, a_posse, h_xg, a_xg, h_chutes, a_chutes, h_chutes_alvo, a_chutes_alvo,
                    h_escanteios, a_escanteios, h_faltas, a_faltas, h_amarelos, a_amarelos,
                    0, 0, h_ataques, a_ataques, h_ataques_p, a_ataques_p,
                    h_defesas, a_defesas,
                    h_passes, a_passes, h_acerto_passe, a_acerto_passe,
                    h_passes_decisivos, a_passes_decisivos, h_cruzamentos, a_cruzamentos,
                    h_dribles, a_dribles,
                    h1_posse, a1_posse, h1_chutes, a1_chutes, h1_chutes_alvo, a1_chutes_alvo,
                    h1_escanteios, a1_escanteios, h1_ataques, a1_ataques, h1_ataques_p, a1_ataques_p,
                    h2_posse, a2_posse, h2_chutes, a2_chutes, h2_chutes_alvo, a2_chutes_alvo,
                    h2_escanteios, a2_escanteios, h2_ataques, a2_ataques, h2_ataques_p, a2_ataques_p,
                    odd_h_open, odd_d_open, odd_a_open, odd_h_close, odd_d_close, odd_a_close
                ))
                conn.commit()
            except Exception as e:
                log.error(f"  -> Erro ao salvar {url}: {e}")

import pandas as pd

def exportar_csv():
    log.info("[EXPORT] Gerando arquivo CSV com os dados coletados...")
    try:
        with sqlite3.connect(DB_NAME) as conn:
            df = pd.read_sql("SELECT * FROM jogos_detalhados", conn)
            df.to_csv("dados_historicos_final.csv", index=False, encoding='utf-8')
            log.info(f"[SUCESSO] CSV gerado com sucesso! Total de registros: {len(df)}")
    except Exception as e:
        log.error(f"[ERRO] Falha ao gerar CSV: {e}")

def rotina_bottom_up():
    inicializar_banco()
    driver = login_redscore(REDSCORE_USER, REDSCORE_PASS)
    
    URL_LIGA = "https://redscores.com/pt-br/league/brazil/serie-a/648?season=25184"
    NOME_LIGA = "Serie A"
    
    try:
        times = fase1_obter_times(driver, URL_LIGA)
        if not times:
            log.error("Nenhum time encontrado. Encerrando.")
            return
            
        log.info("LIMITANDO O TESTE A 2 TIMES PARA VALIDAÇÃO FINAL...")
        times = times[:2]
            
        links_unicos = fase2_obter_links_partidas(driver, times, NOME_LIGA)
        
        if not links_unicos:
            log.error("Nenhum link de partida encontrado. Encerrando.")
            return
            
        log.info(f"Iniciando Fase 3 para todos os {len(links_unicos)} jogos únicos...")
        fase3_raspar_detalhes(driver, links_unicos, NOME_LIGA)
        
        exportar_csv()
        log.info("[SUCESSO] Rotina concluída.")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    rotina_bottom_up()
