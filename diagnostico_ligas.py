#!/usr/bin/env python3
"""
REDSCORE — Motor de Diagnóstico e Qualificação de Ligas por Amostragem.

Esta ferramenta analisa a qualidade de dados de ligas de futebol no Flashscore de forma
rápida (amostragem de ~35 partidas) para mapear a cobertura de xG, Scouts e Odds.
Evita a coleta massiva exaustiva (economizando ~15 horas de scraping) e decide
automaticamente o Tier ideal para novos candidatos.
"""
import sys
import os
import time
import re
import argparse
import logging
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Adiciona o diretório do projeto ao PATH
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.append(str(PROJECT_ROOT))

from config.leagues import LIGAS_FLASHSCORE, LIGAS_PERMITIDAS

# Configura o logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("diagnostico")

BASE_URL = "https://www.flashscore.com.br"
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-fsign': 'SW9D1eZo',
}

STAT_ID_MAP = {
    '432': 'xG', '499': 'xGOT', '34': 'Total_Shots', 
    '13': 'Shots_On_Target', '459': 'Big_Chances', '16': 'Corners', 
    '461': 'Shots_Inside_Box'
}

def slugify(text: str) -> str:
    import unicodedata
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = text.lower()
    text = text.replace("paises baixos", "holanda")
    text = text.replace("eua", "eua")
    text = text.replace("coreia do sul", "coreia-do-sul")
    text = text.replace("pais de gales", "pais-de-gales")
    text = text.replace("irlanda do norte", "irlanda-do-norte")
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

def obter_url_flashscore(nome_liga: str) -> str:
    """Mapeia o nome da liga do Redscore para a URL oficial correspondente do Flashscore."""
    if nome_liga in LIGAS_FLASHSCORE:
        return LIGAS_FLASHSCORE[nome_liga]["url_base"]
    
    # Overrides manuais das 28 ligas delta
    overrides = {
        "África do Sul - Premier League": "https://www.flashscore.com.br/futebol/africa-do-sul/premiership",
        "Albânia - Superliga": "https://www.flashscore.com.br/futebol/albania/super-liga",
        "Argélia - Ligue 1": "https://www.flashscore.com.br/futebol/argelia/1-divisao",
        "Argentina - Primera B Nacional": "https://www.flashscore.com.br/futebol/argentina/primera-nacional",
        "Armênia - Premier League": "https://www.flashscore.com.br/futebol/armenia/premier-league",
        "Bolívia - Liga De Futbol Prof": "https://www.flashscore.com.br/futebol/bolivia/divisao-profissional",
        "Coréia do Sul - K League 2": "https://www.flashscore.com.br/futebol/coreia-do-sul/liga-k-2",
        "Egito - Premier League": "https://www.flashscore.com.br/futebol/egito/primeira-liga",
        "Escócia - Championship": "https://www.flashscore.com.br/futebol/escocia/championship",
        "Escócia - League One": "https://www.flashscore.com.br/futebol/escocia/league-one",
        "Escócia - League Two": "https://www.flashscore.com.br/futebol/escocia/league-two",
        "Eslováquia - Fortuna Liga": "https://www.flashscore.com.br/futebol/eslovaquia/nike-liga",
        "Eslovênia - 1. SNL": "https://www.flashscore.com.br/futebol/eslovenia/prva-liga",
        "Estônia - Meistriliiga": "https://www.flashscore.com.br/futebol/estonia/meistriliiga",
        "Finlândia - Veikkausliiga": "https://www.flashscore.com.br/futebol/finlandia/veikkausliiga",
        "Hungria - OTP Bank Liga": "https://www.flashscore.com.br/futebol/hungria/nb-i",
        "Irlanda - Premier Division": "https://www.flashscore.com.br/futebol/irlanda/divisao-premier",
        "Irlanda do Norte - Premiership": "https://www.flashscore.com.br/futebol/irlanda-do-norte/nifl-premiership",
        "Islândia - Pepsideild": "https://www.flashscore.com.br/futebol/islandia/besta-deild-karla",
        "Israel - Ligat ha'Al": "https://www.flashscore.com.br/futebol/israel/ligat-ha-al",
        "Lituânia - A Lyga": "https://www.flashscore.com.br/futebol/lituania/toplyga",
        "Malásia - Super Liga": "https://www.flashscore.com.br/futebol/malasia/super-liga",
        "País de Gales - Premier League": "https://www.flashscore.com.br/futebol/pais-de-gales/cymru-premier",
        "Paraguai - Division 1": "https://www.flashscore.com.br/futebol/paraguai/copa-de-primera",
        "Peru - Primera Division": "https://www.flashscore.com.br/futebol/peru/liga-1",
        "Sérvia - Super Liga": "https://www.flashscore.com.br/futebol/servia/superliga",
        "Uruguai - Primera Division": "https://www.flashscore.com.br/futebol/uruguai/liga-auf-uruguaia",
        "Venezuela - Primera Division": "https://www.flashscore.com.br/futebol/venezuela/liga-futve",
        "EUA - USL Championship": "https://www.flashscore.com.br/futebol/eua/campeonato-da-usl"
    }
    
    if nome_liga in overrides:
        return overrides[nome_liga]
    
    # Heurística genérica
    if " - " in nome_liga:
        parts = nome_liga.split(" - ")
        pais_slug = slugify(parts[0])
        liga_slug = slugify(parts[1])
        return f"https://www.flashscore.com.br/futebol/{pais_slug}/{liga_slug}"
    
    return f"https://www.flashscore.com.br/futebol/{slugify(nome_liga)}"

def normalizar_para_busca(text: str) -> str:
    import unicodedata
    normalized = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return normalized.upper()

def obter_temporadas(nome_liga: str, pais: str) -> list:
    """Retorna as 3 últimas temporadas da liga analisada."""
    paises_anuais = {
        # Em inglês (como no config do Redscore ou original)
        "BRAZIL", "BRASIL", "USA", "EUA", "SWEDEN", "SUECIA", 
        "NORWAY", "NORUEGA", "JAPAN", "JAPAO", "ARGENTINA", 
        "CHILE", "CHINA", "COLOMBIA", "ECUADOR", "SOUTH KOREA", 
        "COREIA DO SUL", "FINLAND", "FINLANDIA", "ESTONIA", 
        "ICELAND", "ISLANDIA", "LITHUANIA", "LITUANIA", "MALAYSIA", 
        "MALASIA", "PARAGUAY", "PARAGUAI", "PERU", "URUGUAY", 
        "URUGUAI", "VENEZUELA", "BOLIVIA", "IRLANDA", "IRELAND"
    }
    
    if nome_liga in LIGAS_FLASHSCORE:
        base_seasons = LIGAS_FLASHSCORE[nome_liga].get("temporadas", [])
        if base_seasons:
            first = base_seasons[0]
            if "-" in first:
                parts = first.split("-")
                start = int(parts[0])
                end = int(parts[1])
                return [f"{start}-{end}", f"{start-1}-{end-1}", f"{start-2}-{end-2}"]
            else:
                yr = int(first)
                return [str(yr), str(yr-1), str(yr-2)]

    pais_norm = normalizar_para_busca(pais) if pais else ""
    nome_liga_norm = normalizar_para_busca(nome_liga)
    
    is_anual = False
    for pa in paises_anuais:
        if pa in pais_norm or pa in nome_liga_norm:
            is_anual = True
            break
            
    if is_anual:
        return ["2026", "2025", "2024"]
    else:
        return ["2025-2026", "2024-2025", "2023-2024"]

def parse_value(v):
    try: return float(v)
    except: return None

def diagnosticar_partida(match_id: str, session: requests.Session) -> dict:
    """Diagnostica os dados disponíveis para um único match_id via API leve."""
    res = {
        "match_id": match_id,
        "has_odds": False,
        "has_xg": False,
        "has_classic": False,
        "has_advanced": False
    }
    
    # 1. Verificar ODDS (via GraphQL oce)
    url_odds = "https://global.ds.lsapp.eu/odds/pq_graphql"
    params = {"eventId": match_id, "projectId": "401", "geoIpCode": "BR", "geoIpSubdivisionCode": "BR", "_hash": "oce"}
    try:
        r_odds = session.get(url_odds, params=params, headers=HEADERS, timeout=6)
        if r_odds.status_code == 200:
            data = r_odds.json().get("data", {}).get("findOddsByEventId", {}).get("odds", [])
            for market in data:
                bt = market.get("bettingType")
                scope = market.get("bettingScope", "FULL_TIME")
                if bt == "HOME_DRAW_AWAY" and scope == "FULL_TIME":
                    res["has_odds"] = True
                    break
    except Exception as e:
        log.debug(f"Erro ao buscar odds para {match_id}: {e}")

    # 2. Verificar SCOUTS e xG (via df_st_1 feed)
    url_stats = f"{BASE_URL}/x/feed/df_st_1_{match_id}"
    try:
        r_stats = session.get(url_stats, headers={**HEADERS, 'referer': f'{BASE_URL}/jogo/{match_id}/'}, timeout=6)
        if r_stats.status_code == 200 and len(r_stats.text) > 5:
            # Buscar IDs de estatísticas no texto
            stats_found = set(re.findall(r'SD÷(\d+)', r_stats.text))
            
            # xG ID = 432
            if '432' in stats_found:
                res["has_xg"] = True
                
            # Scouts Avançados (xGOT: 499, Big Chances: 459, Shots Inside Box: 461)
            if any(k in stats_found for k in ['499', '459', '461']):
                res["has_advanced"] = True
                
            # Scouts Clássicos (Shots On Target: 13, Corners: 16, Total Shots: 34)
            if any(k in stats_found for k in ['13', '16', '34']):
                res["has_classic"] = True
    except Exception as e:
        log.debug(f"Erro ao buscar stats para {match_id}: {e}")
        
    return res

def diagnosticar_liga(nome_liga: str, driver: webdriver.Chrome, session: requests.Session, sample_total: int = 35) -> dict:
    """Extrai IDs de partidas e audita a qualidade da liga."""
    pais = nome_liga.split(" - ")[0] if " - " in nome_liga else ""
    url_base = obter_url_flashscore(nome_liga)
    temporadas = obter_temporadas(nome_liga, pais)
    
    # Distribuir amostra pelas 3 temporadas (ex: 15 / 10 / 10)
    amostras_por_temp = {}
    if len(temporadas) >= 3:
        amostras_por_temp[temporadas[0]] = max(5, sample_total - 20)
        amostras_por_temp[temporadas[1]] = 10
        amostras_por_temp[temporadas[2]] = 10
    else:
        for t in temporadas:
            amostras_por_temp[t] = max(5, sample_total // len(temporadas))
            
    log.info(f"🔍 Iniciando diagnóstico para '{nome_liga}'...")
    log.info(f"   -> URL Base: {url_base}")
    log.info(f"   -> Temporadas: {amostras_por_temp}")
    
    todos_match_ids = []
    
    for temp, qtd in amostras_por_temp.items():
        is_newest = (temp == temporadas[0])
        url_resultados = f"{url_base}/resultados/" if is_newest else f"{url_base}-{temp}/resultados/"
        
        try:
            driver.get(url_resultados)
            time.sleep(3) # Aguarda renderização dos jogos
            
            # Captura os IDs loaded sem clicar em Mostrar Mais
            match_ids = driver.execute_script(r"""
                const items = document.querySelectorAll('.event__match');
                const ids = [];
                items.forEach(item => {
                    const mid = item.id.split('_').pop();
                    if (mid) ids.push(mid);
                });
                return ids;
            """)
            
            if match_ids:
                log.info(f"   [{temp}] Encontradas {len(match_ids)} partidas na primeira página. Amostrando {min(qtd, len(match_ids))}...")
                todos_match_ids.extend([(mid, temp) for mid in match_ids[:qtd]])
            else:
                log.warning(f"   [{temp}] Nenhuma partida encontrada na URL: {url_resultados}")
        except Exception as e:
            log.error(f"   [{temp}] Falha ao ler página de resultados: {e}")
            
    if not todos_match_ids:
        log.error(f"❌ Falha completa: Nenhuma partida encontrada para '{nome_liga}' em nenhuma temporada.")
        return {
            "liga": nome_liga,
            "url": url_base,
            "status": "Erro (Sem partidas)",
            "odds_cov": 0.0,
            "xg_cov": 0.0,
            "classic_cov": 0.0,
            "advanced_cov": 0.0,
            "total_amostrado": 0,
            "tier_sugerido": "Descartar"
        }
        
    log.info(f"   -> Analisando {len(todos_match_ids)} partidas no total...")
    resultados_partidas = []
    
    for mid, temp in tqdm(todos_match_ids, desc=f"Auditando {nome_liga}", leave=False):
        res_partida = diagnosticar_partida(mid, session)
        res_partida["temporada"] = temp
        resultados_partidas.append(res_partida)
        time.sleep(0.2) # Proteção rate-limit
        
    # Agregação estatística
    total = len(resultados_partidas)
    odds_count = sum(1 for r in resultados_partidas if r["has_odds"])
    xg_count = sum(1 for r in resultados_partidas if r["has_xg"])
    classic_count = sum(1 for r in resultados_partidas if r["has_classic"])
    adv_count = sum(1 for r in resultados_partidas if r["has_advanced"])
    
    odds_cov = (odds_count / total) * 100
    xg_cov = (xg_count / total) * 100
    classic_cov = (classic_count / total) * 100
    adv_cov = (adv_count / total) * 100
    
    # Classificação em Tiers
    if odds_cov >= 95.0 and xg_cov >= 90.0:
        tier = "Tier 1 (Elite)"
    elif odds_cov >= 95.0 and classic_cov >= 80.0:
        tier = "Tier 2 (Acesso)"
    elif odds_cov >= 90.0 and classic_cov >= 50.0:
        tier = "Tier 3 (Exótica)"
    else:
        tier = "Descartar (Baixa Qualidade)"
        
    log.info(f"   🏆 Resultado: Odds {odds_cov:.1f}% | xG {xg_cov:.1f}% | Scouts {classic_cov:.1f}% | Tier: {tier}")
    
    return {
        "liga": nome_liga,
        "url": url_base,
        "status": "Sucesso",
        "odds_cov": round(odds_cov, 1),
        "xg_cov": round(xg_cov, 1),
        "classic_cov": round(classic_cov, 1),
        "advanced_cov": round(adv_cov, 1),
        "total_amostrado": total,
        "tier_sugerido": tier
    }

def main():
    parser = argparse.ArgumentParser(description="Auditor de Qualidade de Ligas por Amostragem")
    parser.add_argument("--leagues", type=str, default="", help="Ligas específicas para auditar (separadas por vírgula)")
    parser.add_argument("--sample-size", type=int, default=35, help="Tamanho total da amostragem por liga (padrão: 35)")
    args = parser.parse_args()
    
    # Determinar a lista de ligas a analisar
    ligas_alvo = []
    if args.leagues:
        ligas_alvo = [l.strip() for l in args.leagues.split(",") if l.strip()]
    else:
        # Se nenhuma liga for passada, analisa todas as LIGAS_PERMITIDAS do Redscore
        # que ainda NÃO estão integradas nas ligas ativas de LIGAS_FLASHSCORE
        ligas_integradas = set(LIGAS_FLASHSCORE.keys())
        ligas_alvo = sorted(list(LIGAS_PERMITIDAS - ligas_integradas))
        
    if not ligas_alvo:
        log.info("Nenhuma nova liga candidata pendente de diagnóstico.")
        return
        
    log.info("=" * 60)
    log.info("      📊 MOTOR DE DIAGNÓSTICO DE COBERTURA - REDSCORE V3")
    log.info("=" * 60)
    log.info(f" -> Ligas candidatas na fila: {len(ligas_alvo)} ligas")
    log.info(f" -> Amostra por liga:         {args.sample_size} jogos")
    log.info("=" * 60)
    
    # Inicializa Selenium Headless
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    import os

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    except Exception as e:
        log.warning(f"Erro ao inicializar com ChromeDriverManager: {e}. Tentando fallback padrão...")
        try:
            driver = webdriver.Chrome(options=options)
        except Exception as e2:
            log.warning(f"Erro no fallback padrão: {e2}. Tentando usar /usr/bin/chromedriver explicitamente...")
            if os.path.exists("/usr/bin/chromedriver"):
                driver = webdriver.Chrome(service=Service(executable_path="/usr/bin/chromedriver"), options=options)
            else:
                raise e2
    session = requests.Session()
    
    relatorios = []
    
    try:
        for idx, liga in enumerate(ligas_alvo, 1):
            print(f"\n[{idx}/{len(ligas_alvo)}] ----------------------------------------")
            rel = diagnosticar_liga(liga, driver, session, sample_total=args.sample_size)
            relatorios.append(rel)
    finally:
        driver.quit()
        
    # Salvar relatório Markdown de auditoria
    data_dir = PROJECT_ROOT / "data"
    data_dir.mkdir(exist_ok=True)
    report_file = data_dir / "diagnostico_qualidade_ligas.md"
    
    df = pd.DataFrame(relatorios)
    
    # Criar conteúdo Markdown premium
    md_content = f"""# 📊 Relatório de Auditoria e Diagnóstico de Qualidade das Ligas

Este relatório apresenta o diagnóstico de qualidade dos dados coletados por amostragem para novas ligas candidatas a inclusão no Redscore.
* **Data da Execução:** {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}
* **Tamanho da Amostra por Liga:** {args.sample_size} jogos (distribuídos nas últimas 3 temporadas)

---

## 📋 Tabela Comparativa de Cobertura e Tiering

| Liga | Cobertura Odds (%) | Cobertura xG (%) | Cobertura Scouts (%) | Cobertura xGOT/Box (%) | Jogos Amostrados | Status | Tier Sugerido |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
    
    for r in relatorios:
        md_content += f"| {r['liga']} | {r['odds_cov']}% | {r['xg_cov']}% | {r['classic_cov']}% | {r['advanced_cov']}% | {r['total_amostrado']} | {r['status']} | **{r['tier_sugerido']}** |\n"
        
    md_content += """
---

## 🧠 Diretrizes para Decisão de Inclusão

1. **Tier 1 (Elite):**
   * *Critério:* Odds ≥ 95% e xG ≥ 90%.
   * *Ação:* Inclusão recomendada imediatamente com suporte a estatísticas avançadas e xG de alta fidelidade no treinamento de Machine Learning.
   
2. **Tier 2 (Acesso):**
   * *Critério:* Odds ≥ 95%, xG < 90% e Scouts Clássicos ≥ 80%.
   * *Ação:* Inclusão recomendada usando modelagem híbrida (apenas scouts tradicionais + movimentação de odds, sem pesos de xG ou descartando xG nulos no Pandas).
   
3. **Tier 3 (Exótica):**
   * *Critério:* Odds ≥ 90% e Scouts Clássicos ≥ 50%.
   * *Ação:* Excelente para caçar ineficiências em casas de apostas menores. Evitar xG e focar em cantos, gols HT/FT e ELO de força.

4. **Descartar:**
   * *Critério:* Odds < 80% ou Scouts Clássicos < 30%.
   * *Ação:* Não incluir no banco de dados. Os dados são esparsos ou incompletos, o que polui os pipelines de modelagem.
"""
    
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(md_content)
        
    log.info("=" * 60)
    log.info(f"✅ Diagnóstico concluído! Relatório salvo em: {report_file}")
    log.info("=" * 60)
    
    # Mostrar resumo rápido no terminal
    print("\n🏆 RESUMO RÁPIDO DO TIERING RECOMENDADO:")
    for r in relatorios:
        print(f" - {r['liga']}: {r['tier_sugerido']} (Odds: {r['odds_cov']}% | xG: {r['xg_cov']}% | Scouts: {r['classic_cov']}%)")
    print("=" * 60)

if __name__ == "__main__":
    main()
