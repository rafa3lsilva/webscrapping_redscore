import asyncio
import os
import re
import csv
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

# ============================================
# CONFIGURAÇÃO
# ============================================

URL_RESULTADOS = "https://www.flashscore.com.br/futebol/brasil/brasileirao-betano-2025/resultados/"
URL_JOGO_BASE = "https://www.flashscore.com.br/jogo/{id}/#/resumo/"
CHROME_PATH = "/usr/bin/google-chrome"
OUTPUT_FILE = "historical/data/resultados_br_2025.csv"

# Reduzido para evitar bloqueios e falhas de carregamento
CONCURRENCY_LIMIT = 5 

# Limite de jogos para teste (None para pegar todos)
TEST_LIMIT = 10 

# ============================================
# FUNÇÕES DE APOIO
# ============================================

def extrair_id_limpo(id_raw):
    if not id_raw: return ""
    return id_raw.replace("g_1_", "")

def extrair_gols_ht(txt):
    if not txt: return "", ""
    m = re.search(r'(\d+)\s*[-–—]\s*(\d+)', txt)
    if m:
        return m.group(1), m.group(2)
    return "", ""

async def carregar_tudo(page):
    """Clica no botão 'Mostrar mais jogos' até o botão desaparecer"""
    print("\n[1/3] Carregando histórico completo (Resultados)...")
    selector_botao = "#tournamentPage > div:nth-child(2) > section > div.wcl-footer_yI6S3 > button"
    
    while True:
        try:
            botao = page.locator(selector_botao)
            if await botao.is_visible():
                await botao.click()
                await asyncio.sleep(2.5) # Aumentado para garantir carregamento
            else:
                await asyncio.sleep(1)
                if not await page.locator(selector_botao).is_visible():
                    break
        except:
            break
    print("      OK: Histórico carregado.")

async def extrair_detalhes_jogo(browser_context, jogo, sem):
    """Acessa a página de detalhes de um jogo específico"""
    mid = jogo["Match_ID"]
    async with sem:
        page = await browser_context.new_page()
        # User Agent para evitar bloqueios
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        })
        
        try:
            # Tenta carregar com retry e espera maior
            for attempt in range(3):
                try:
                    await page.goto(URL_JOGO_BASE.format(id=mid), wait_until="domcontentloaded", timeout=30000)
                    # Espera o seletor chave carregar
                    await page.wait_for_selector(".duelParticipant__startTime", timeout=10000)
                    break
                except:
                    if attempt == 2: raise
                    await asyncio.sleep(2) # Espera antes de tentar de novo

            # 1. Hora Precisa
            try:
                dt_full = await page.locator(".duelParticipant__startTime").inner_text(timeout=5000)
                partes = dt_full.strip().split(" ")
                jogo["Data"] = partes[0] if len(partes) > 0 else ""
                jogo["Hora"] = partes[1] if len(partes) > 1 else ""
            except:
                pass

            # 2. Gols HT
            try:
                # Espera as abas ou o sumário aparecer
                await page.wait_for_selector(".tabContent__match-summary", timeout=5000)
                ht_el = page.locator(".tabContent__match-summary section > div:nth-child(1) > div > div:nth-child(1)").first
                ht_raw = await ht_el.inner_text(timeout=3000)
                h_ht, a_ht = extrair_gols_ht(ht_raw)
                jogo["H_Gols_HT"], jogo["A_Gols_HT"] = h_ht, a_ht
            except:
                pass

        except Exception:
            pass
        finally:
            await page.close()

# ============================================
# EXECUÇÃO PRINCIPAL
# ============================================

async def run():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    async with async_playwright() as p:
        print("Conectando ao navegador...")
        browser = await p.chromium.launch(executable_path=CHROME_PATH, headless=True)
        # Contexto com User Agent global
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        
        main_page = await context.new_page()
        print(f"Acessando: {URL_RESULTADOS}")
        await main_page.goto(URL_RESULTADOS, wait_until="networkidle")
        
        # Aceitar Cookies
        try: await main_page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
        except: pass

        # 1. Carregar Histórico
        await carregar_tudo(main_page)

        # 2. Metadados da Liga
        try:
            pais_raw = await main_page.locator(".headerLeague__category-text").inner_text()
            liga_raw = await main_page.locator(".headerLeague__title").inner_text()
            pais = pais_raw.replace(":", "").strip().upper()
            liga = liga_raw.strip().upper()
        except:
            pais, liga = "BRASIL", "BRASILEIRÃO"

        # 3. Extrair lista de IDs
        print("\n[2/3] Mapeando jogos da lista...")
        elementos = await main_page.locator(".event__round, .event__match").all()
        lista_ids = []
        rodada_atual = "N/A"

        for el in elementos:
            classes = await el.get_attribute("class") or ""
            if "event__round" in classes:
                rodada_atual = await el.inner_text()
                rodada_atual = rodada_atual.strip().upper()
                continue
            
            if "event__match" in classes:
                match_id = extrair_id_limpo(await el.get_attribute("id"))
                home = await el.locator(".event__homeParticipant").inner_text()
                away = await el.locator(".event__awayParticipant").inner_text()
                h_ft = await el.locator(".event__score--home").inner_text()
                a_ft = await el.locator(".event__score--away").inner_text()

                lista_ids.append({
                    "Match_ID": match_id,
                    "Data": "", "Hora": "", 
                    "Pais": pais, "Liga": liga, "Rodada": rodada_atual,
                    "Home": home.replace("\n", " ").strip(),
                    "Away": away.replace("\n", " ").strip(),
                    "H_Gols_FT": h_ft.strip(), "A_Gols_FT": a_ft.strip(),
                    "H_Gols_HT": "", "A_Gols_HT": ""
                })

        # 4. Coleta Paralela de Detalhes
        if TEST_LIMIT:
            print(f"\n[!] MODO TESTE ATIVADO: Limitando a {TEST_LIMIT} jogos.")
            lista_ids = lista_ids[:TEST_LIMIT]

        print(f"\n[3/3] Coletando detalhes de {len(lista_ids)} jogos...")
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [extrair_detalhes_jogo(context, jogo, sem) for jogo in lista_ids]
        
        # Barra de progresso tqdm
        await tqdm.gather(*tasks, desc="Extraindo Detalhes", unit="jogo")

        # 5. Salvar CSV (Ordem atualizada: Match_ID em primeiro)
        if lista_ids:
            colunas = ["Match_ID", "Data", "Hora", "Pais", "Liga", "Rodada", "Home", "Away", "H_Gols_FT", "A_Gols_FT", "H_Gols_HT", "A_Gols_HT"]
            with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=colunas)
                writer.writeheader()
                writer.writerows(lista_ids)
            print(f"\n[SUCESSO] {len(lista_ids)} jogos salvos em: {OUTPUT_FILE}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
