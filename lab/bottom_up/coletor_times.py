import sqlite3
import time
import logging
import os
import sys
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Permite importar módulos do projeto raiz
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from collector.auth.credentials import REDSCORE_USER, REDSCORE_PASS
from collector.auth.login import login_redscore
from config import leagues as cfg
from urllib.parse import urljoin
from tqdm import tqdm

# Configuração de Logs
import unicodedata

def normalize_str(s):
    if not s: return ""
    return "".join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn').lower()

# 1. Configuração do Log em Arquivo (Detalhado)
logging.basicConfig(
    filename='coletor_times.log',
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    filemode='a'
)
log = logging.getLogger("bottom_up")

# 2. Configuração do Terminal (Limpo e Resumido)
# Remove handlers anteriores se houver (para evitar duplicidade ao recarregar)
if log.hasHandlers():
    log.handlers.clear()

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S')
console.setFormatter(console_format)
log.addHandler(console)

DB_NAME = "dados_historicos_novo.db"

def inicializar_banco():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jogos_detalhados (
            Link TEXT PRIMARY KEY,
            Data TEXT, Hora TEXT, Pais TEXT, Liga TEXT, Rodada TEXT,
            Home TEXT, Away TEXT,
            H_gols_ft INTEGER, A_gols_ft INTEGER,
            H_gols_ht INTEGER, A_gols_ht INTEGER,
            H_posse REAL, A_posse REAL,
            H_xg REAL, A_xg REAL,
            H_chutes INTEGER, A_chutes INTEGER,
            H_chutes_alvo INTEGER, A_chutes_alvo INTEGER,
            H_escanteios INTEGER, A_escanteios INTEGER,
            H_faltas INTEGER, A_faltas INTEGER,
            H_amarelos INTEGER, A_amarelos INTEGER,
            H_vermelhos INTEGER, A_vermelhos INTEGER,
            H_ataques INTEGER, A_ataques INTEGER,
            H_ataques_perigosos INTEGER, A_ataques_perigosos INTEGER,
            H_defesas_goleiro INTEGER, A_defesas_goleiro INTEGER,
            H_passes INTEGER, A_passes INTEGER,
            H_acerto_passe REAL, A_acerto_passe REAL,
            H_passes_decisivos INTEGER, A_passes_decisivos INTEGER,
            H_cruzamentos INTEGER, A_cruzamentos INTEGER,
            H_dribles INTEGER, A_dribles INTEGER,
            
            -- 1º Tempo
            H1_chutes_alvo INTEGER, A1_chutes_alvo INTEGER,
            H1_ataques INTEGER, A1_ataques INTEGER, H1_ataques_perigosos INTEGER, A1_ataques_perigosos INTEGER,
            
            -- 2º Tempo
            H2_chutes_alvo INTEGER, A2_chutes_alvo INTEGER,
            H2_ataques INTEGER, A2_ataques INTEGER, H2_ataques_perigosos INTEGER, A2_ataques_perigosos INTEGER,

            Odd_h_open REAL, Odd_d_open REAL, Odd_a_open REAL,
            Odd_h_close REAL, Odd_d_close REAL, Odd_a_close REAL,
            UNIQUE(Rodada, Data, Home, Away)
        )""")
        
        # Migração: Adicionar coluna Pais se não existir em bancos antigos
        try:
            conn.execute("ALTER TABLE jogos_detalhados ADD COLUMN Pais TEXT")
            log.info("Coluna 'Pais' adicionada com sucesso ao banco de dados.")
        except sqlite3.OperationalError:
            pass # A coluna já existe
            
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

def fase0_obter_mapeamento_ligas(driver, ligas_permitidas_set):
    log.info("[FASE 0] Mapeando URLs das ligas configuradas em ligas_config.py...")
    driver.get("https://redscores.com/pt-br/leagues")
    time.sleep(3)
    
    # Tentativa de clicar em "Minhas Ligas" (Favoritos)
    try:
        # Seletor robusto via classe específica do site
        btn_minhas_ligas = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.showMyLeagues"))
        )
        driver.execute_script("arguments[0].click();", btn_minhas_ligas)
        log.info("  -> Aba 'Minhas Ligas' clicada com sucesso.")
        time.sleep(5) # Espera carregar a lista de favoritos
    except Exception as e:
        log.warning(f"  -> Não foi possível clicar em 'Minhas Ligas': {e}")
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    mapeamento = {}
    permitidas_norm = {normalize_str(l): l for l in ligas_permitidas_set}
    
    # 1. Varredura de Links de Ligas
    # Prioriza o container de favoritos se existir
    links_candidatos = soup.select('#snippet--favList a[href*="/pt-br/league/"]')
    if not links_candidatos:
        links_candidatos = soup.select('a[href*="/pt-br/league/"]')

    for a in links_candidatos:
        # Tenta pegar o texto visível primeiro
        liga_text = a.get_text(" ", strip=True)
        
        # Se estiver vazio, tenta o atributo 'alt' de uma imagem interna
        if not liga_text:
            img = a.select_one("img")
            if img and img.get('alt'):
                liga_text = img['alt'].strip()
        
        # Se ainda vazio, tenta o 'title' do link
        if not liga_text:
            liga_text = a.get('title', '').strip()
            
        # Fallback para o parent 
        if not liga_text or len(liga_text) < 2:
            parent = a.find_parent()
            if parent:
                # Remove o texto de ícones/estrelas se possível
                liga_text = parent.get_text(" ", strip=True)
        
        if not liga_text or len(liga_text) < 2: 
            continue
        
        url = urljoin("https://redscores.com", a['href']).split('?')[0]
        
        # Tenta achar o país (geralmente em um botão ou span próximo)
        pais_text = ""
        current = a
        for _ in range(5):
            if not current: break
            btn = current.find_previous_sibling("button", class_="btn-link") or \
                  (current.find_parent("div").select_one("button.btn-link") if current.find_parent("div") else None)
            if btn:
                pais_text = btn.get_text(strip=True)
                break
            current = current.parent
        n_liga = normalize_str(liga_text)
        
        # 1. Casamento Exato
        if n_liga in permitidas_norm:
            mapeamento[permitidas_norm[n_liga]] = url
            continue
            
        # 2. Casamento por Sufixo (ex: "Ekstraklasa" -> "Polônia - Ekstraklasa")
        found_suffix = False
        for n_perm, original in permitidas_norm.items():
            if n_perm.endswith(" - " + n_liga):
                mapeamento[original] = url
                found_suffix = True
                break
        if found_suffix: continue

        # 3. "País - Liga" (se o país foi detectado no DOM)
        if pais_text:
            n_completo = normalize_str(f"{pais_text} - {liga_text}")
            if n_completo in permitidas_norm:
                mapeamento[permitidas_norm[n_completo]] = url
                continue
                
        # 4. "Contém" (último recurso se for um favorito e o nome for único o suficiente)
        for n_perm, original in permitidas_norm.items():
            if n_liga in n_perm and original not in mapeamento:
                mapeamento[original] = url
                break
    # 2. Varredura Global (Tenta encontrar o que sobrou na aba "Todas as Ligas")
    if len(mapeamento) < len(ligas_permitidas_set):
        log.info(f"  -> {len(ligas_permitidas_set) - len(mapeamento)} ligas ainda não encontradas. Tentando aba 'Todas as Ligas'...")
        try:
            # Tenta clicar na primeira aba (geralmente 'Todas as Ligas' ou 'Populares')
            btn_todas = driver.find_element(By.CSS_SELECTOR, "ul.nav-tabs li:first-child a, a.showAllLeagues")
            driver.execute_script("arguments[0].click();", btn_todas)
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            for a in soup.select('a[href*="/pt-br/league/"]'):
                liga_text = a.get_text(" ", strip=True)
                if not liga_text:
                    img = a.select_one("img")
                    if img and img.get('alt'): liga_text = img['alt'].strip()
                
                if not liga_text or len(liga_text) < 2: continue
                url = urljoin("https://redscores.com", a['href']).split('?')[0]
                n_liga = normalize_str(liga_text)
                
                if n_liga not in mapeamento:
                    # Casamento por sufixo ou contém
                    for n_perm, original in permitidas_norm.items():
                        if original not in mapeamento.values():
                            if n_perm.endswith(" - " + n_liga) or n_liga == n_perm or n_liga in n_perm:
                                mapeamento[original] = url
                                break
        except Exception as e:
            log.warning(f"  -> Erro ao tentar varredura global: {e}")

    # Relatório Final da Fase 0
    faltando = [l for l in ligas_permitidas_set if l not in mapeamento]
    if faltando:
        log.warning(f"[FASE 0] Ligas não encontradas ({len(faltando)}): {faltando[:5]}...")
    
    log.info(f"[FASE 0] Mapeamento concluído. {len(mapeamento)} ligas encontradas de {len(ligas_permitidas_set)} configuradas.")
    return mapeamento

def fase2_obter_links_partidas(driver, urls_times, liga_nome):
    log.info(f"[FASE 2] Iniciando mineração de partidas para {len(urls_times)} times.")
    links_partidas = set()
    wait = WebDriverWait(driver, 10)
    
    for idx, team_url in enumerate(tqdm(urls_times, desc="Minerando Times", unit="time"), 1):
        # Limpeza do link (Remover pt-br se causar erro de página em branco)
        # O subagent detectou que links sem o prefixo de idioma são mais estáveis
        clean_url = team_url.replace("/pt-br/team/", "/team/")
        
        log.debug(f"  -> Explorando Time {idx}/{len(urls_times)}: {clean_url}")
        driver.get(clean_url)
        
        try:
            # Espera a tabela ou o botão de "Ver Mais" aparecer
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.match-grid__bottom, .link-see-more")))
        except:
            log.warning(f"  -> Tabela não carregou para {clean_url}. Tentando fallback...")
            # Se falhou, tenta com o URL original
            if clean_url != team_url:
                driver.get(team_url)
                time.sleep(3)
            else:
                continue
            
        # Tentar clicar no filtro de Liga na página do time para não pegar amistosos/copas
        try:
            # Seletor sugerido pelo usuário e um fallback genérico
            selectores_filtro = [
                "#snippet--teamForm > div > div > div.match-grid__left > div > div > table > thead > tr > td > div > div > div > a:nth-child(2)",
                "div.match-grid__filters a:nth-child(2)"
            ]
            for sel in selectores_filtro:
                try:
                    filtro = driver.find_element(By.CSS_SELECTOR, sel)
                    if filtro.is_displayed():
                        driver.execute_script("arguments[0].click();", filtro)
                        #log.info(f"  -> Filtro de liga aplicado ({sel}).")
                        time.sleep(2)
                        break
                except: continue
        except: pass
        # Filtro de liga - Agora clicamos em "Todas as Ligas" (Geralmente a 1ª opção)
        # para capturar jogos de outras ligas permitidas (ex: Serie B enquanto fazemos Serie A)
        try:
            liga_filter_selector = "div.match-grid__filters a:nth-child(1)"
            filtro = driver.find_element(By.CSS_SELECTOR, liga_filter_selector)
            if filtro.is_displayed():
                driver.execute_script("arguments[0].click();", filtro)
                log.debug(f"  -> Filtro 'Todas as Ligas' aplicado.")
                time.sleep(2)
        except:
            log.debug("  -> Filtro de ligas não encontrado ou padrão já é 'Todas'.")

        # Scroll/See More (Expansão Agressiva)
        click_count = 0
        while click_count < 20: # Aumentado para 20 para pegar mais histórico
            try:
                # Tenta vários seletores possíveis para o botão "Ver Mais"
                selectors = [".link-see-more", "a.match-grid__more", "a[class*='more']"]
                btn = None
                for sel in selectors:
                    try:
                        found = driver.find_element(By.CSS_SELECTOR, sel)
                        if found.is_displayed():
                            btn = found
                            break
                    except: continue
                
                if btn:
                    driver.execute_script("arguments[0].click();", btn)
                    click_count += 1
                    time.sleep(1.2)
                else:
                    break
            except:
                break
        
        if click_count > 0:
            log.debug(f"  -> Tabela expandida {click_count} vezes.")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Filtra os links pela liga desejada
        encontrados_time = 0
        duplicados_time = 0
        liga_nome_norm = normalize_str(liga_nome)
        
        for tr in soup.select("div.match-grid__bottom tbody tr"):
            tds = tr.find_all('td')
            if len(tds) < 2:
                continue
                
            img = tds[1].find('img')
            liga_local = img['alt'].strip() if img and 'alt' in img.attrs else ""
            liga_local_norm = normalize_str(liga_local)
            
            if liga_nome_norm in liga_local_norm:
                a_tag = tr.select_one("a[href*='/match/']")
                if a_tag:
                    href = a_tag['href']
                    if not href.startswith("http"):
                        href = "https://redscores.com" + href
                    
                    if href in links_partidas:
                        duplicados_time += 1
                    else:
                        links_partidas.add(href)
                    encontrados_time += 1
                    
        log.debug(f"  -> {encontrados_time} rows de liga encontradas. {duplicados_time} eram links repetidos.")
        
    log.info(f"[FASE 2] Concluída. {len(links_partidas)} partidas ÚNICAS encontradas no total.")
    return list(links_partidas)

def fase3_raspar_detalhes(driver, urls_partidas, nome_liga_esperada, pais_esperado=""):
    log.info(f"[FASE 3] Iniciando Deep Scraping de {len(urls_partidas)} partidas...")
    
    with sqlite3.connect(DB_NAME) as conn:
        for idx, url in enumerate(tqdm(urls_partidas, desc=f"Lendo {nome_liga_esperada}", unit="jogo"), 1):
            # 1. Verificação ultra-rápida pelo LINK antes de abrir o navegador
            # Normalização básica para bater mesmo se houver /pt-br/ ou falta de #ID
            url_canonical = url.replace("https://redscores.com", "").replace("/pt-br", "").split("#")[0]
            log.debug(f"  -> Buscando no banco: %{url_canonical}%")
            res_link = conn.execute(
                "SELECT 1 FROM jogos_detalhados WHERE Link LIKE ?", 
                (f"%{url_canonical}%",)
            ).fetchone()
            
            if res_link:
                log.debug(f"  -> Jogo {idx}/{len(urls_partidas)} já existe (Link: {url_canonical}). Pulando...")
                continue

            log.debug(f"  -> Coletando Partida {idx}/{len(urls_partidas)}: {url}")
            driver.get(url)
            time.sleep(1.5)
            
            # Scroll para carregar estatísticas
            driver.execute_script("window.scrollTo(0, 800);")
            time.sleep(1)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # --- FILTRO DE LIGA E EXTRAÇÃO DE PAÍS (BREADCRUMBS) ---
            # Verificação de Liga Permitida (Flexível)
            breadcrumbs = soup.select('li.breadcrumb__item a')
            pais_real = pais_esperado
            if len(breadcrumbs) >= 3:
                pais_real = breadcrumbs[1].get_text(strip=True)
                liga_real = breadcrumbs[2].get_text(strip=True)
                n_liga_real = normalize_str(liga_real)
                
                # Verifica se a liga do jogo está na lista de permitidas (como nome completo ou sufixo)
                # Ex: "Serie A" bate com "Brasil - Serie A"
                is_permitida = False
                for liga_perm in cfg.LIGAS_PERMITIDAS:
                    n_perm = normalize_str(liga_perm)
                    if n_liga_real == n_perm or n_perm.endswith(" - " + n_liga_real):
                        is_permitida = True
                        break
                
                # Também checa se é a liga específica que estamos processando agora (caso não esteja no config global por algum motivo)
                if not is_permitida and nome_liga_esperada.lower() in liga_real.lower():
                    is_permitida = True

                if not is_permitida:
                    log.debug(f"  -> Pulando liga não configurada: {liga_real}")
                    continue

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
                        raw_date = partes[0] if len(partes) > 0 else ""
                        hora_str = partes[1] if len(partes) > 1 else ""
                        
                        # Converter MM/DD/YY para AAAA-MM-DD
                        if raw_date and '/' in raw_date:
                            try:
                                # Assume MM/DD/YY
                                m, d, y = raw_date.split('/')
                                if len(y) == 2: y = "20" + y # 26 -> 2026
                                data_str = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
                            except:
                                data_str = raw_date
                    else:
                        partes = txt.split(' ')
                        data_str = partes[0] if len(partes) > 0 else ""
                        hora_str = partes[1] if len(partes) > 1 else ""
            
            # Normalizar Rodada
            if rodada:
                num_rodada = ''.join(filter(str.isdigit, rodada))
                if num_rodada:
                    rodada = f"Rodada {num_rodada}"
            
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
            
            # Verificação de Duplicidade ANTES de carregar estatísticas
            res = conn.execute(
                "SELECT 1 FROM jogos_detalhados WHERE Rodada=? AND Data=? AND Home=? AND Away=?",
                (rodada, data_str, home, away)
            ).fetchone()
            if res:
                log.debug(f"  -> Jogo {idx}/{len(urls_partidas)} já existe no banco: {home} x {away} ({rodada}). Pulando...")
                continue

            log.debug(f"  -> Raspando Jogo {idx}/{len(urls_partidas)}: {home} x {away} (Data: {data_str}, R: {rodada})")
            
            ht_div = soup.find(lambda tag: tag.name == "td" and "half-time" in tag.get("class", []) and "FT" in tag.text)
            placar_ht = ht_div.get_text(strip=True).replace(" FT", "") if ht_div else ""

            # --- ESTATÍSTICAS ---
            stats_ft = {}
            stats_1h = {}
            stats_2h = {}
            
            def parse_stats_to_dict(s):
                d = {}
                # 1. Padrão Circular (Posse de bola)
                # Geralmente: <span class="progress-title">Posse de bola</span> + .progress-container
                for pt in s.select('.progress-title'):
                    title = pt.get_text(strip=True)
                    parent = pt.parent
                    container = parent.select_one('.progress-container')
                    if container:
                        v_left = container.select_one('.progress-value-left')
                        v_right = container.select_one('.progress-value-right')
                        if v_left and v_right:
                            val_l = v_left.get_text(strip=True)
                            val_r = v_right.get_text(strip=True)
                            d[title] = (val_l, val_r)
                            # log.debug(f"    [PARSER] Detectado Circular: {title} -> {val_l} | {val_r}")

                # 2. Padrão Barras (xG, Chutes, etc)
                # Geralmente: .stats-ranking-item contendo título e .progress-bar__rail
                for item in s.select('.stats-ranking-item'):
                    rail = item.select_one('.progress-bar__rail')
                    if rail:
                        # O título é o texto do item menos o texto do rail
                        title = item.get_text(" ", strip=True).replace(rail.get_text(" ", strip=True), "").strip()
                        v_left = rail.select_one('.progress-bar__value_left')
                        v_right = rail.select_one('.progress-bar__value_right')
                        if v_left and v_right:
                            val_l = v_left.get_text(strip=True)
                            val_r = v_right.get_text(strip=True)
                            d[title] = (val_l, val_r)
                            # log.debug(f"    [PARSER] Detectado Barra: {title} -> {val_l} | {val_r}")
                return d

            def get_val(d, key, is_home, default="0"):
                for k, vals in d.items():
                    if key.lower() in k.lower():
                        v = str(vals[0] if is_home else vals[1]).strip()
                        # Lida com formatos como '309(84)'
                        if '(' in v and ')' in v:
                            if "acerto" in key.lower() or "posse" in key.lower():
                                # Prioriza o que está dentro do parêntese (geralmente a %)
                                v = v.split('(')[1].split(')')[0]
                            else:
                                # Pega o valor principal fora do parêntese
                                v = v.split('(')[0]
                        
                        v = v.replace('%', '').strip()
                        try:
                            return float(v)
                        except:
                            return float(default)
                return float(default)

            def get_val_split(d, key, is_home):
                for k, vals in d.items():
                    if key.lower() in k.lower():
                        v = vals[0] if is_home else vals[1]
                        return int(v.split('/')[0]) if '/' in str(v) else int(v)
                return 0

            # Coleção por período (Jogo, 1T, 2T)
            # Segundo inspeção: Jogo=All, 1T=ht, 2T=ft
            periods = [("All", stats_ft), ("ht", stats_1h), ("ft", stats_2h)]
            for x_val, d_target in periods:
                try:
                    # Tentar encontrar por xfcg-value primeiro
                    btn = None
                    try:
                        btn = driver.find_element(By.XPATH, f"//a[@xfcg-value='{x_val}']")
                    except:
                        # Fallback por texto se o xfcg-value mudar
                        if x_val == "All": btn = driver.find_element(By.XPATH, "//a[contains(text(), 'Jogo')]")
                        elif x_val == "ht": btn = driver.find_element(By.XPATH, "//a[contains(text(), '1º Tempo')]")
                        elif x_val == "ft": btn = driver.find_element(By.XPATH, "//a[contains(text(), '2º TEMPO')]")
                    
                    if btn:
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2) # Espera carregar
                    
                    if x_val == "All":
                        # No Jogo Completo, temos as sub-abas Ataque (off) e Defesa (def)
                        # 1. Ataque (Geralmente padrão, mas clicamos para garantir)
                        try:
                            ataque_btn = driver.find_element(By.XPATH, "//a[@xfcg-value='off']")
                            driver.execute_script("arguments[0].click();", ataque_btn)
                            time.sleep(1)
                        except: pass
                        soup_atk = BeautifulSoup(driver.page_source, 'html.parser')
                        d_target.update(parse_stats_to_dict(soup_atk))
                        
                        # 2. Defesa
                        try:
                            def_btn = driver.find_element(By.XPATH, "//a[@xfcg-value='def']")
                            driver.execute_script("arguments[0].click();", def_btn)
                            time.sleep(1)
                            soup_def = BeautifulSoup(driver.page_source, 'html.parser')
                            d_target.update(parse_stats_to_dict(soup_def))
                            
                            # Volta para Ataque para não bugar nada
                            try:
                                driver.execute_script("arguments[0].click();", ataque_btn)
                                time.sleep(0.5)
                            except: pass
                        except: pass
                    else:
                        # 1T e 2T não têm sub-abas Defesa/Ataque no RedScore
                        current_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        d_target.update(parse_stats_to_dict(current_soup))
                    
                    if x_val == "All":
                        # Cartões (Geralmente visíveis em qualquer aba do Jogo Completo)
                        current_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        # Procuramos os containers de cartões dos dois times
                        card_divs = [div for div in current_soup.select("div.col-4.text-center") if "Cartões" in div.text]
                        
                        for i, div in enumerate(card_divs):
                            prefix = "H" if i == 0 else "A" # Primeiro é Home, segundo é Away
                            for counter in div.select(".cards-counter"):
                                use_tag = counter.select_one("use")
                                if not use_tag: continue
                                href = use_tag.get("xlink:href", "") or use_tag.get("href", "")
                                val = counter.select_one(".cards-counter__number").get_text(strip=True) if counter.select_one(".cards-counter__number") else "0"
                                
                                if "#yellow-card" in href:
                                    d_target[f"{prefix}_Amarelos"] = val
                                elif "#red-card" in href:
                                    d_target[f"{prefix}_Vermelhos"] = val
                except Exception as e:
                    log.warning(f"  -> Erro ao processar período {x_val}: {e}")
                    pass

            # --- MAPEAMENTO FINAL ---
            h_gols_ft = a_gols_ft = 0
            if '-' in placar_ft:
                parts = placar_ft.split('-')
                if len(parts) == 2:
                    h_gols_ft = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
                    a_gols_ft = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                    
            h_gols_ht = a_gols_ht = 0
            if '-' in placar_ht:
                parts = placar_ht.split('-')
                if len(parts) == 2:
                    h_gols_ht = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
                    a_gols_ht = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                    
            h_posse, a_posse = get_val(stats_ft, "Posse", True), get_val(stats_ft, "Posse", False)
            h_xg, a_xg = get_val(stats_ft, "xG", True), get_val(stats_ft, "xG", False)
            h_chutes, a_chutes = int(get_val(stats_ft, "Total de chutes", True)), int(get_val(stats_ft, "Total de chutes", False))
            h_chutes_alvo, a_chutes_alvo = int(get_val(stats_ft, "Chutes a gol", True)), int(get_val(stats_ft, "Chutes a gol", False))
            h_escanteios, a_escanteios = int(get_val(stats_ft, "Escanteios", True)), int(get_val(stats_ft, "Escanteios", False))
            h_faltas, a_faltas = int(get_val(stats_ft, "Faltas", True)), int(get_val(stats_ft, "Faltas", False))
            h_amarelos = int(stats_ft.get("H_Amarelos", 0))
            a_amarelos = int(stats_ft.get("A_Amarelos", 0))
            h_vermelhos = int(stats_ft.get("H_Vermelhos", 0))
            a_vermelhos = int(stats_ft.get("A_Vermelhos", 0))
            h_ataques, a_ataques = int(get_val(stats_ft, "Ataques", True)), int(get_val(stats_ft, "Ataques", False))
            h_ataques_p, a_ataques_p = int(get_val(stats_ft, "Ataques Perigosos", True)), int(get_val(stats_ft, "Ataques Perigosos", False))
            h_defesas, a_defesas = int(get_val(stats_ft, "Defesas do goleiro", True)), int(get_val(stats_ft, "Defesas do goleiro", False))
            h_passes, a_passes = int(get_val(stats_ft, "Total de passes", True)), int(get_val(stats_ft, "Total de passes", False))
            h_acerto_passe, a_acerto_passe = get_val(stats_ft, "Acerto no passe", True), get_val(stats_ft, "Acerto no passe", False)
            h_passes_decisivos, a_passes_decisivos = int(get_val(stats_ft, "Passes decisivos", True)), int(get_val(stats_ft, "Passes decisivos", False))
            h_cruzamentos, a_cruzamentos = get_val_split(stats_ft, "Cruzamentos", True), get_val_split(stats_ft, "Cruzamentos", False)
            h_dribles, a_dribles = get_val_split(stats_ft, "Dribles", True), get_val_split(stats_ft, "Dribles", False)

            h1_chutes_alvo, a1_chutes_alvo = int(get_val(stats_1h, "Chutes a gol", True)), int(get_val(stats_1h, "Chutes a gol", False))
            h1_ataques, a1_ataques = int(get_val(stats_1h, "Ataques", True)), int(get_val(stats_1h, "Ataques", False))
            h1_ataques_p, a1_ataques_p = int(get_val(stats_1h, "Ataques Perigosos", True)), int(get_val(stats_1h, "Ataques Perigosos", False))

            h2_chutes_alvo, a2_chutes_alvo = int(get_val(stats_2h, "Chutes a gol", True)), int(get_val(stats_2h, "Chutes a gol", False))
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
                (Link, Data, Hora, Pais, Liga, Rodada, Home, Away, H_gols_ft, A_gols_ft, H_gols_ht, A_gols_ht,
                 H_posse, A_posse, H_xg, A_xg, H_chutes, A_chutes, H_chutes_alvo, A_chutes_alvo,
                 H_escanteios, A_escanteios, H_faltas, A_faltas, H_amarelos, A_amarelos,
                 H_vermelhos, A_vermelhos, H_ataques, A_ataques, H_ataques_perigosos, A_ataques_perigosos,
                 H_defesas_goleiro, A_defesas_goleiro, 
                 H_passes, A_passes, H_acerto_passe, A_acerto_passe,
                 H_passes_decisivos, A_passes_decisivos, H_cruzamentos, A_cruzamentos,
                 H_dribles, A_dribles,
                 H1_chutes_alvo, A1_chutes_alvo, H1_ataques, A1_ataques, H1_ataques_perigosos, A1_ataques_perigosos,
                 H2_chutes_alvo, A2_chutes_alvo, H2_ataques, A2_ataques, H2_ataques_perigosos, A2_ataques_perigosos,
                 Odd_h_open, Odd_d_open, Odd_a_open, Odd_h_close, Odd_d_close, Odd_a_close)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    url, data_str, hora_str, pais_real, nome_liga_esperada, rodada, home, away, h_gols_ft, a_gols_ft, h_gols_ht, a_gols_ht,
                    h_posse, a_posse, h_xg, a_xg, h_chutes, a_chutes, h_chutes_alvo, a_chutes_alvo,
                    h_escanteios, a_escanteios, h_faltas, a_faltas, h_amarelos, a_amarelos,
                    h_vermelhos, a_vermelhos, h_ataques, a_ataques, h_ataques_p, a_ataques_p,
                    h_defesas, a_defesas,
                    h_passes, a_passes, h_acerto_passe, a_acerto_passe,
                    h_passes_decisivos, a_passes_decisivos, h_cruzamentos, a_cruzamentos,
                    h_dribles, a_dribles,
                    h1_chutes_alvo, a1_chutes_alvo, h1_ataques, a1_ataques, h1_ataques_p, a1_ataques_p,
                    h2_chutes_alvo, a2_chutes_alvo, h2_ataques, a2_ataques, h2_ataques_p, a2_ataques_p,
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
    
    try:
        # 1. Mapear as URLs das ligas configuradas (MODO MANUAL)
        mapeamento_ligas = getattr(cfg, 'URLS_MANUAIS', {}).copy()
        
        if not mapeamento_ligas:
            log.error("Nenhuma liga encontrada em URLS_MANUAIS do ligas_config.py. Encerrando.")
            return

        for idx, (nome_amigavel, url_liga) in enumerate(mapeamento_ligas.items(), 1):
            log.info(f"\n{'='*60}\nINICIANDO COLETA DA LIGA {idx}/{len(mapeamento_ligas)}: {nome_amigavel}\n{'='*60}")
            
            # Reiniciar navegador a cada 10 ligas para evitar vazamento de memória e quedas
            if idx > 1 and idx % 10 == 0:
                log.info("  -> Reiniciando navegador para manter estabilidade...")
                try: driver.quit()
                except: pass
                driver = login_redscore(REDSCORE_USER, REDSCORE_PASS)

            # Verificar se o driver ainda está respondendo
            try:
                _ = driver.current_url
            except Exception:
                log.warning("  -> WebDriver parou de responder. Reiniciando sessão...")
                try: driver.quit()
                except: pass
                driver = login_redscore(REDSCORE_USER, REDSCORE_PASS)
            
            # O nome da liga para filtro de links é o que vem depois do " - "
            partes_nome = nome_amigavel.split(" - ")
            pais = partes_nome[0] if len(partes_nome) > 1 else ""
            nome_liga_curto = partes_nome[-1] if len(partes_nome) > 1 else nome_amigavel

            try:
                times = fase1_obter_times(driver, url_liga)
                if not times:
                    log.warning(f"  -> Nenhum time encontrado for {nome_amigavel}. Pulando...")
                    continue
                
                links_unicos = fase2_obter_links_partidas(driver, times, nome_liga_curto)
                
                if not links_unicos:
                    log.warning(f"  -> Nenhum link de partida encontrado for {nome_amigavel}. Pulando...")
                    continue
                    
                log.info(f"Iniciando Fase 3 for todos os {len(links_unicos)} jogos únicos da liga {nome_amigavel}...")
                fase3_raspar_detalhes(driver, links_unicos, nome_liga_curto, pais)
            
            # Exportação parcial a cada liga concluída
                exportar_csv()
            
            except Exception as e:
                log.error(f"Erro crítico ao processar liga {nome_amigavel}: {e}")
                continue

        log.info(f"\n{'='*60}\nCOLETA DE TODAS AS LIGAS CONCLUÍDA!\n{'='*60}")

    finally:
        driver.quit()

if __name__ == "__main__":
    rotina_bottom_up()
