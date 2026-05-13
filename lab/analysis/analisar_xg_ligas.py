import sqlite3
import time
import logging
import re
import sys
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
import unicodedata
from urllib.parse import urljoin

from auth_redscore import REDSCORE_USER, REDSCORE_PASS
from login_redscore import login_redscore
import ligas_config as cfg

# Configuração de Log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("analise_xg.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("analise_xg")

def normalize_str(s):
    if not s: return ""
    return "".join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn').lower()

def extrair_urls_comentadas(caminho_arquivo):
    urls = {}
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            content = f.read()
            # Match: #"Nome": "URL"
            matches = re.findall(r'#\s*"([^"]+)":\s*"([^"]+)"', content)
            for nome, url in matches:
                urls[nome] = url
    except Exception as e:
        log.error(f"Erro ao extrair URLs comentadas: {e}")
    return urls

def obter_links_recentes(driver, url_liga):
    """Obtém os links das últimas partidas de uma liga."""
    try:
        driver.get(url_liga)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        links = []
        # Tenta encontrar links de partidas
        for a in soup.select('a[href*="/match/"]'):
            href = a['href']
            if not href.startswith("http"):
                href = urljoin("https://redscores.com", href)
            if href not in links:
                links.append(href)
            if len(links) >= 8: # Pega 8 para ter margem
                break
        return links
    except Exception as e:
        log.error(f"Erro ao obter links para {url_liga}: {e}")
        return []

def verificar_dados_na_partida(driver, url_partida):
    """
    Verifica se uma partida específica possui dados de xG e Odds (Abertura/Fechamento).
    Retorna (tem_xg, tem_odds_open, tem_odds_close)
    """
    try:
        driver.get(url_partida)
        time.sleep(1.5)
        
        # Scroll para carregar conteúdos dinâmicos
        driver.execute_script("window.scrollTo(0, 800);")
        time.sleep(0.5)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        text = soup.get_text()
        
        # 1. Verificação de xG
        tem_xg = False
        if "xG" in text or "Expected Goals" in text:
            if soup.select_one('.stats-ranking-item') or soup.select_one('.progress-container'):
                for item in soup.select('.stats-ranking-item, .progress-container'):
                    if "xG" in item.get_text() or "Expected Goals" in item.get_text():
                        tem_xg = True
                        break

        # 2. Verificação de Odds (Abertura e Fechamento)
        tem_odds_open = False
        tem_odds_close = False
        
        # Procura a tabela de odds
        odds_table = soup.select_one("table.table-data__table--odds")
        if odds_table:
            for tr in odds_table.select("tbody tr"):
                th = tr.find('th')
                if not th: continue
                txt_th = th.get_text(strip=True).lower()
                
                # Verifica se existem 3 valores numéricos na linha (1 X 2)
                tds = tr.find_all('td')
                if len(tds) >= 3:
                    try:
                        # Tenta converter pelo menos um valor para float para garantir que não é "-" ou vazio
                        v1 = tds[0].get_text(strip=True).replace('↑', '').replace('↓', '')
                        if float(v1) > 1.0:
                            if "abertura" in txt_th: tem_odds_open = True
                            elif "antes" in txt_th or "fechamento" in txt_th or "clausura" in txt_th:
                                tem_odds_close = True
                    except: pass

        return tem_xg, tem_odds_open, tem_odds_close
    except Exception as e:
        log.debug(f"Erro ao verificar dados em {url_partida}: {e}")
        return False, False, False

def main():
    test_mode = "--test" in sys.argv
    
    driver = login_redscore(REDSCORE_USER, REDSCORE_PASS)
    if not driver:
        log.error("Falha ao iniciar o driver.")
        return

    try:
        # Mapeamento inicial
        mapeamento = getattr(cfg, 'URLS_MANUAIS', {}).copy()
        
        # Extrai as comentadas também para não precisar navegar tanto
        urls_comentadas = extrair_urls_comentadas("ligas_config.py")
        for nome, url in urls_comentadas.items():
            if nome not in mapeamento:
                mapeamento[nome] = url

        # Se ainda faltar alguma liga de LIGAS_PERMITIDAS, tenta mapear via site
        faltando = [l for l in cfg.LIGAS_PERMITIDAS if l not in mapeamento]
        if faltando and not test_mode:
            log.info(f"Mapeando {len(faltando)} ligas restantes via RedScore...")
            driver.get("https://redscores.com/pt-br/leagues")
            time.sleep(3)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            permitidas_norm = {normalize_str(l): l for l in faltando}
            
            for a in soup.select('a[href*="/pt-br/league/"]'):
                liga_text = a.get_text(" ", strip=True)
                if not liga_text: continue
                url = urljoin("https://redscores.com", a['href']).split('?')[0]
                n_liga = normalize_str(liga_text)
                if n_liga in permitidas_norm:
                    mapeamento[permitidas_norm[n_liga]] = url

        # Filtrar para teste se necessário
        items = list(mapeamento.items())
        if test_mode:
            log.info("MODO TESTE: Analisando apenas as 5 primeiras ligas.")
            items = items[:5]
        
        resultados = []
        
        for nome_liga, url_liga in tqdm(items, desc="Analisando Ligas"):
            log.info(f"Analisando: {nome_liga}")
            
            links_partidas = obter_links_recentes(driver, url_liga)
            
            # Contadores para esta liga
            jogos_com_xg = 0
            jogos_com_odds_open = 0
            jogos_com_odds_close = 0
            jogos_analisados = 0
            
            max_jogos = 5 # Aumentado para 5 conforme solicitado
            
            for url_partida in links_partidas:
                tem_xg, tem_open, tem_close = verificar_dados_na_partida(driver, url_partida)
                
                if tem_xg: jogos_com_xg += 1
                if tem_open: jogos_com_odds_open += 1
                if tem_close: jogos_com_odds_close += 1
                
                jogos_analisados += 1
                if jogos_analisados >= max_jogos: 
                    break
            
            # Métricas finais da liga
            # xG é SIM se pelo menos 1 jogo tiver (ou podemos ser mais rigorosos, mas aqui manteremos SIM se houver presença)
            status_xg = "SIM" if jogos_com_xg > 0 else "NÃO"
            
            # % de Odds (em relação aos jogos analisados)
            perc_open = (jogos_com_odds_open / jogos_analisados * 100) if jogos_analisados > 0 else 0
            perc_close = (jogos_com_odds_close / jogos_analisados * 100) if jogos_analisados > 0 else 0
            
            resultados.append({
                "nome": nome_liga,
                "status_xg": status_xg,
                "perc_open": perc_open,
                "perc_close": perc_close,
                "url": url_liga
            })
            log.info(f"Resultado para {nome_liga}: xG={status_xg}, Odds Open={perc_open:.0f}%, Odds Close={perc_close:.0f}%")

        # Gerar Relatório Markdown
        relatorio_path = "relatorio_xg_ligas.md"
        with open(relatorio_path, "w", encoding="utf-8") as f:
            f.write("# Relatório de Disponibilidade de Dados por Liga\n\n")
            f.write(f"*Gerado em: {time.strftime('%d/%m/%Y %H:%M:%S')}*\n\n")
            f.write("| Liga | Possui xG? | % Odds Open | % Odds Close | URL |\n")
            f.write("| :--- | :---: | :---: | :---: | :--- |\n")
            for res in sorted(resultados, key=lambda x: x['nome']):
                f.write(f"| {res['nome']} | **{res['status_xg']}** | {res['perc_open']:.0f}% | {res['perc_close']:.0f}% | [Link]({res['url']}) |\n")
        
        log.info(f"Análise concluída. Relatório gerado: {relatorio_path}")

        # Gerar Dashboard HTML automaticamente
        try:
            from gerar_dashboard_xg import gerar_html
            gerar_html()
        except Exception as e:
            log.warning(f"Não foi possível gerar o dashboard HTML: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
