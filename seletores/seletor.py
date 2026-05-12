import os
from playwright.sync_api import sync_playwright
import json
import time
import re
import csv

# ============================================
# CONFIG
# ============================================

URL_BASE = "https://www.flashscore.com.br/jogo/futebol/athletico-pr-UoAxb1Tq/vasco-2RABlYFn/resumo/"
CHROME_PATH = "/usr/bin/google-chrome"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

MAPA_NOMES = {
    "esperados": "xG",
    "xgot": "xGOT",
    "posse": "Posse",
    "finalizacoes": "Chutes",
    "alvo": "Chutes_Alvo",
    "bloqueados": "Chutes_Bloqueados",
    "fora": "Chutes_Fora",
    "chances": "Chances_Claras",
    "area": "Toques_Area",
    "escanteios": "Escanteios",
    "defesas": "Defesas",
    "faltas": "Faltas",
    "impedimentos": "Impedimentos",
    "ataques": "Ataques",
    "perigosos": "Ataques_Perigosos",
    "amarelos": "Cartoes_Amarelos",
    "vermelhos": "Cartoes_Vermelhos"
}

# ============================================
# FUNÇÕES
# ============================================

def limpar_valor(val):
    if val is None or val == "": return None
    s_val = str(val).replace("%", "").strip()
    match = re.search(r"(\d+\.?\d*)", s_val)
    if match:
        num = match.group(1)
        return float(num) if "." in num else int(num)
    return None

def get_path(filename):
    return os.path.join(SCRIPT_DIR, filename)

def extrair_stats_da_view(page, label_aba):
    print(f"  [Scan] Analisando métricas da aba: {label_aba}...")
    try:
        # Espera as linhas de estatística aparecerem (Classe exata confirmada pelo subagente)
        page.wait_for_selector('div.wcl-row_2oCpS', timeout=15000)
        
        # Espera o carregamento de um campo chave para garantir que os dados estão lá
        try: page.wait_for_selector('text="Posse de bola"', timeout=10000)
        except: pass
        
        page.wait_for_timeout(2000)
        
        # CAPTURA (Usa o seletor definitivo row_2oCpS)
        all_stats = page.evaluate("""() => {
            const res = {};
            const rows = document.querySelectorAll('div.wcl-row_2oCpS');
            rows.forEach(row => {
                const lines = row.innerText.split('\\n').map(s => s.trim()).filter(s => s);
                if (lines.length >= 3) {
                    const valH = lines[0], valA = lines[lines.length - 1];
                    const catNome = lines.slice(1, -1).join(' ').toLowerCase();
                    res[catNome] = { home: valH, away: valA };
                }
            });
            return res;
        }""")
        
        # Captura EXTRA para xGOT (Seletor do usuário como prioridade)
        try:
            user_xgot_sel = '#detail > div.tabContent__match-summary > div.tabContent__match-statistics > div.sectionsWrapper > div:nth-child(2) > div:nth-child(3) > div.wcl-category_Ydwqh'
            xgot_el = page.locator(user_xgot_sel).first
            if xgot_el.count():
                txt = xgot_el.inner_text().split('\n')
                if len(txt) >= 3:
                    all_stats["xgot_priority"] = {"home": limpar_valor(txt[0]), "away": limpar_valor(txt[2])}
                    print(f"    -> [Extra] xGOT detectado via seletor específico.")
        except: pass
        
        for cat in all_stats:
            all_stats[cat]["home"] = limpar_valor(all_stats[cat].get("home", 0))
            all_stats[cat]["away"] = limpar_valor(all_stats[cat].get("away", 0))
            
        print(f"    [OK] {len(all_stats)} métricas capturadas em '{label_aba}'.")
        return all_stats
    except Exception as e:
        print(f"    [!] Erro ao escanear '{label_aba}': {e}")
    return {}

def salvar_csv_final(resultado):
    def normalizar(txt):
        if not txt: return ""
        # Remove acentos e caracteres não-alfanuméricos
        s = "".join(c for c in unicodedata.normalize('NFD', txt) if unicodedata.category(c) != 'Mn').lower()
        return re.sub(r'[^a-z0-9]', '', s)

    print("\nGerando CSV final...")
    try:
        h = resultado["header"]
        rodada_raw = h.get("rodada", "")
        rodada_final = f"RODADA {rodada_raw}" if rodada_raw.isdigit() else rodada_raw.upper()

        row = {
            "Match_ID": resultado["match_id"], "Data": h.get("data", ""), "Hora": h.get("hora", ""),
            "Pais": h.get("pais", "BRASIL"), "Liga": h.get("liga", "BRASILEIRÃO BETANO"), "Temporada": "2025",
            "Rodada": rodada_final, "Home": h.get("home", ""), "Away": h.get("away", ""),
            "Gols_Home_FT": h.get("score_home"), "Gols_Away_FT": h.get("score_away"),
            "Gols_Home_HT": h.get("ht_home"), "Gols_Away_HT": h.get("ht_away")
        }

        MAPA = {
            "xg": "xG", "xgot": "xGOT", "priority": "xGOT", "posse": "Posse",
            "finalizacoes": "Chutes", "chutes": "Chutes", "alvo": "Chutes_Alvo",
            "escanteios": "Escanteios", "chances": "Chances_Claras", "area": "Toques_Area",
            "defesas": "Defesas", "faltas": "Faltas", "impedimentos": "Impedimentos",
            "ataques": "Ataques", "perigosos": "Ataques_Perigosos", "amarelos": "Cartoes_Amarelos"
        }

        # Inicializar todas as colunas de estatísticas como None para garantir ordem no CSV
        for pref in ["FT", "HT", "2T"]:
            for col_val in ["xG", "xGOT", "Posse", "Chutes", "Chutes_Alvo", "Escanteios", "Chances_Claras", "Toques_Area", "Defesas", "Faltas", "Impedimentos", "Ataques", "Ataques_Perigosos", "Cartoes_Amarelos"]:
                row[f"{pref}_{col_val}_Home"] = None
                row[f"{pref}_{col_val}_Away"] = None

        # Preencher os valores capturados
        for p_key, p_prefix in {"total": "FT", "1_tempo": "HT", "2_tempo": "2T"}.items():
            stats_raw = resultado["statistics"].get(p_key, {})
            if not stats_raw: continue
            
            for raw_name, values in stats_raw.items():
                name_norm = normalizar(raw_name)
                mapped_key = None
                
                # PRIORIDADE TOTAL xGOT
                if "xgot" in name_norm or "alvo" in name_norm or "priority" in name_norm: 
                    mapped_key = "xGOT"
                elif "xg" in name_norm: 
                    mapped_key = "xG"
                elif "posse" in name_norm:
                    mapped_key = "Posse"
                elif "escanteio" in name_norm:
                    mapped_key = "Escanteios"
                elif "finalizacao" in name_norm or "chute" in name_norm:
                    if "alvo" in name_norm: mapped_key = "Chutes_Alvo"
                    else: mapped_key = "Chutes"
                else:
                    for k, v in MAPA.items():
                        if k in name_norm:
                            mapped_key = v
                            break
                
                if mapped_key:
                    col_h, col_a = f"{p_prefix}_{mapped_key}_Home", f"{p_prefix}_{mapped_key}_Away"
                    row[col_h], row[col_a] = values["home"], values["away"]
                    print(f"    [Map] {p_prefix}_{mapped_key} -> {values['home']} x {values['away']}")
                else:
                    # Log de métricas não mapeadas para debug
                    print(f"    [!] Não mapeado: '{raw_name}'")

        output_file = get_path("estatisticas_flashscore.csv")
        with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writeheader()
            writer.writerow(row)
        print(f"  [OK] CSV salvo com sucesso! ({len(row)} campos)")
    except Exception as e: print(f"  [Erro CSV] {e}")

# ============================================
# MAIN
# ============================================

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=CHROME_PATH, headless=True)
        # VIEWPORT GIGANTE (1920x5000) para carregar tudo sem rolar
        context = browser.new_context(viewport={"width": 1920, "height": 5000})
        page = context.new_page()
        
        try:
            print(f"Iniciando: {URL_BASE}")
            page.goto(URL_BASE, wait_until="load", timeout=60000)
            
            # Aceitar Cookies
            try: page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
            except: pass
            
            page.wait_for_timeout(5000)

            header = {"pais": "BRASIL", "liga": "BRASILEIRÃO BETANO"}
            
            # 1. Metadados e Gols HT
            try:
                header["home"] = page.locator(".duelParticipant__home .participant__participantName").first.inner_text().strip()
                header["away"] = page.locator(".duelParticipant__away .participant__participantName").first.inner_text().strip()
                
                pl_txt = page.locator(".duelParticipant__score .detailScore__wrapper").first.inner_text()
                gols_ft = re.findall(r"\d+", pl_txt)
                if len(gols_ft) >= 2:
                    header["score_home"], header["score_away"] = int(gols_ft[0]), int(gols_ft[1])
                
                # PLACAR HT via Seletor do Usuário
                ht_h, ht_a = 0, 0
                try:
                    user_sel = "#detail > div.tabContent__match-summary > div.tabContent__match-summary > section > div:nth-child(2) > div > div:nth-child(1)"
                    ht_text = page.locator(user_sel).inner_text()
                    ht_m = re.search(r'(\d+)\s*[-–—]\s*(\d+)', ht_text)
                    if ht_m:
                        ht_h, ht_a = int(ht_m.group(1)), int(ht_m.group(2))
                except: pass
                header["ht_home"], header["ht_away"] = ht_h, ht_a
                
                # Rodada e Data
                full_text = page.evaluate("document.body.innerText")
                rod_m = re.search(r"RODADA\s*(\d+)", full_text, re.IGNORECASE)
                if rod_m: header["rodada"] = rod_m.group(1)
                
                dt_m = re.search(r"(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2})", full_text)
                if dt_m:
                    header["data"], header["hora"] = dt_m.group(1), dt_m.group(2)
            except: pass

            mid = "unknown"
            match_id_res = re.search(r'-([a-zA-Z0-9]{8})/', URL_BASE)
            if match_id_res: mid = match_id_res.group(1)
            resultado = {"match_id": mid, "header": header, "statistics": {}}

            # 2. Estatísticas
            print("Capturando Estatísticas...")
            # 2. Estatísticas
            print("Capturando Estatísticas...")
            try:
                # GATILHO MESTRE: Clique via JS (único que funciona 100%)
                page.evaluate("""() => {
                    const btn = document.querySelector('a[href*="estatisticas"]');
                    if (btn) btn.click();
                }""")
                page.wait_for_timeout(5000)
                print("  [OK] Aba Estatísticas aberta via Gatilho JS.")
            except Exception as e:
                print(f"  [!] Erro ao disparar Gatilho: {e}")

            # Loop pelas sub-abas (Jogo, 1º Tempo, 2º Tempo)
            for aba in [{"nome": "total", "label": "Jogo"}, {"nome": "1_tempo", "label": "1º tempo"}, {"nome": "2_tempo", "label": "2º tempo"}]:
                print(f"Processando: {aba['label']}")
                try:
                    # Clique na sub-aba via JS
                    page.evaluate(f"""(label) => {{
                        const tabs = Array.from(document.querySelectorAll('a, div')).filter(el => el.innerText.includes(label));
                        if (tabs.length > 0) tabs[0].click();
                    }}""", aba['label'])
                    page.wait_for_timeout(3000)
                    
                    resultado["statistics"][aba["nome"]] = extrair_stats_da_view(page, aba["label"])
                except Exception as e:
                    print(f"  [!] Erro na aba {aba['label']}: {e}")

            salvar_csv_final(resultado)
            print("\nConcluído!")

        except Exception as e: print(f"Erro: {e}")
        finally: browser.close()

if __name__ == "__main__":
    run()