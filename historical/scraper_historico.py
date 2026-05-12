import os
import re
import time
import csv
from playwright.sync_api import sync_playwright

# ============================================
# CONFIGURAÇÃO
# ============================================

URL_RESULTADOS = "https://www.flashscore.com.br/futebol/brasil/brasileirao-betano-2025/resultados/"
URL_JOGO_BASE = "https://www.flashscore.com.br/jogo/{id}/#/resumo/"
CHROME_PATH = "/usr/bin/google-chrome"
OUTPUT_FILE = "historical/data/resultados_br_2025.csv"

# ============================================
# FUNÇÕES DE APOIO
# ============================================

def extrair_id_limpo(id_raw):
    """Converte 'g_1_ABC123' em 'ABC123'"""
    if not id_raw: return ""
    return id_raw.replace("g_1_", "")

def extrair_gols_ht(txt):
    """Retorna (home, away) de strings como '1º TEMPO 1 - 0'"""
    if not txt: return None, None
    m = re.search(r'(\d+)\s*[-–—]\s*(\d+)', txt)
    if m:
        return m.group(1), m.group(2)
    return None, None

def carregar_tudo(page):
    """Clica no botão 'Mostrar mais jogos' até o botão desaparecer"""
    print("Iniciando carregamento do histórico completo...")
    selector_botao = "#tournamentPage > div:nth-child(2) > section > div.wcl-footer_yI6S3 > button"
    
    while True:
        try:
            botao = page.locator(selector_botao)
            if botao.is_visible():
                print("  [+] Clicando em 'Mostrar mais jogos'...")
                botao.click()
                # Aguarda um tempo para carregar e o botão reaparecer ou sumir
                page.wait_for_timeout(2500)
            else:
                # Verifica se realmente sumiu ou se apenas demorou a carregar
                page.wait_for_timeout(1000)
                if not page.locator(selector_botao).is_visible():
                    print("  [OK] Todo o histórico carregado (botão desapareceu).")
                    break
        except Exception as e:
            print(f"  [!] Fim do carregamento ou botão não encontrado: {e}")
            break

# ============================================
# EXECUÇÃO PRINCIPAL
# ============================================

def run():
    # Garante que o diretório de dados existe
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with sync_playwright() as p:
        print(f"Conectando ao navegador...")
        # Modo HEADLESS ativado conforme solicitado
        browser = p.chromium.launch(executable_path=CHROME_PATH, headless=True)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()
        
        try:
            print(f"Acessando: {URL_RESULTADOS}")
            page.goto(URL_RESULTADOS, wait_until="networkidle")
            
            # Aceitar Cookies se aparecer
            try: page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
            except: pass

            # 1. Carregar todos os jogos (Histórico)
            carregar_tudo(page)

            # 2. Extrair Metadados da Liga
            try:
                pais = page.locator(".headerLeague__category-text").inner_text().strip()
                liga_nome = page.locator(".headerLeague__title").inner_text().strip()
                campeonato = f"{pais}: {liga_nome}"
            except:
                campeonato = "Desconhecido"
            
            print(f"Iniciando extração: {campeonato}")

            # 3. Extrair dados respeitando a ordem de rodadas
            # Buscamos tanto rodadas quanto partidas para manter a associação
            elementos = page.locator(".event__round, .event__match").all()
            total_elementos = len(elementos)
            print(f"Detectados {total_elementos} elementos (rodadas + jogos).")
            
            lista_ids = []
            rodada_atual = "N/A"

            # 1ª Passagem: Coletar IDs e dados básicos da lista
            for el in elementos:
                try:
                    classes = el.get_attribute("class") or ""
                    if "event__round" in classes:
                        rodada_atual = el.inner_text().strip()
                        continue
                    
                    if "event__match" in classes:
                        match_id = extrair_id_limpo(el.get_attribute("id"))
                        home_team = el.locator(".event__homeParticipant").inner_text().strip()
                        away_team = el.locator(".event__awayParticipant").inner_text().strip()
                        score_home = el.locator(".event__score--home").inner_text().strip()
                        score_away = el.locator(".event__score--away").inner_text().strip()

                        lista_ids.append({
                            "Match_ID": match_id,
                            "Campeonato": campeonato,
                            "Rodada": rodada_atual,
                            "Home": home_team.replace("\n", " ").strip(),
                            "Away": away_team.replace("\n", " ").strip(),
                            "Score_Home": score_home,
                            "Score_Away": score_away,
                            "Data": "",
                            "Hora": "",
                            "H_Gols_HT": "",
                            "A_Gols_HT": ""
                        })
                except: continue

            # 2ª Passagem: Visitar cada jogo para detalhes (Hora precisa e HT)
            print(f"Iniciando coleta detalhada de {len(lista_ids)} jogos...")
            dados_finais = []
            
            for i, jogo in enumerate(lista_ids):
                mid = jogo["Match_ID"]
                # Log a cada 20 jogos para não poluir o terminal
                if (i + 1) % 20 == 0 or (i + 1) == len(lista_ids):
                    print(f"  Progresso: {i+1}/{len(lista_ids)} jogos processados...")
                
                try:
                    # Usando networkidle para garantir que as estatísticas carreguem
                    page.goto(URL_JOGO_BASE.format(id=mid), wait_until="networkidle", timeout=30000)
                    
                    # Aguarda o elemento de tempo aparecer
                    page.wait_for_selector(".duelParticipant__startTime", timeout=10000)
                    
                    # Extrair Hora Precisa
                    try:
                        dt_full = page.locator(".duelParticipant__startTime").inner_text(timeout=5000).strip()
                        partes = dt_full.split(" ")
                        jogo["Data"] = partes[0] if len(partes) > 0 else ""
                        jogo["Hora"] = partes[1] if len(partes) > 1 else ""
                    except:
                        jogo["Data"], jogo["Hora"] = "N/A", "N/A"

                    # Extrair Gols HT (Aba Sumário)
                    try:
                        # Garante que estamos vendo o sumário (onde fica o HT)
                        page.wait_for_selector(".tabContent__match-summary", timeout=5000)
                        ht_el = page.locator(".tabContent__match-summary section > div:nth-child(1) > div > div:nth-child(1)").first
                        if ht_el.is_visible():
                            ht_raw = ht_el.inner_text()
                            h_ht, a_ht = extrair_gols_ht(ht_raw)
                            jogo["H_Gols_HT"], jogo["A_Gols_HT"] = h_ht, a_ht
                    except:
                        pass

                    dados_finais.append(jogo)
                except Exception as e:
                    # Log apenas se for erro real de navegação
                    print(f"    [!] Erro de conexão no jogo {mid}")
                    dados_finais.append(jogo)

            # 4. Salvar em CSV
            if dados_finais:
                colunas = ["Match_ID", "Campeonato", "Rodada", "Home", "Away", "Score_Home", "Score_Away", "Data", "Hora", "H_Gols_HT", "A_Gols_HT"]
                with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=colunas)
                    writer.writeheader()
                    writer.writerows(dados_finais)
                print(f"\n[SUCESSO] {len(dados_finais)} jogos salvos em: {OUTPUT_FILE}")
            else:
                print("\n[!] Nenhum jogo encontrado.")

        except Exception as e:
            print(f"Erro Geral: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    run()
