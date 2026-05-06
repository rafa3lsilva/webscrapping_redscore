import sqlite3
import time
import random
import logging
import pandas as pd
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import data as dt
from login_redscore import login_redscore
import ligas_config as cfg
from auth_redscore import REDSCORE_USER, REDSCORE_PASS

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("coletor_historico.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("historico")

NOME_DB_HISTORICO = "dados_historicos.db"
LIMITE_RODADAS = 3  # Defina um número (ex: 5) para limitar, ou None para coletar todas

def inicializar_banco_historico():
    with sqlite3.connect(NOME_DB_HISTORICO) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jogos (
            Data TEXT, Home TEXT, Away TEXT, Liga TEXT, Temporada TEXT, Rodada TEXT,
            H_Gols_FT INTEGER, A_Gols_FT INTEGER, H_Gols_HT INTEGER, A_Gols_HT INTEGER, 
            H_Chute INTEGER, A_Chute INTEGER, H_Chute_Gol INTEGER, A_Chute_Gol INTEGER, 
            H_Ataques INTEGER, A_Ataques INTEGER, H_Escanteios INTEGER, A_Escanteios INTEGER, 
            Odd_H REAL, Odd_D REAL, Odd_A REAL,
            PRIMARY KEY (Data, Home, Away)
        )""")
        conn.commit()
    log.info(f"Banco {NOME_DB_HISTORICO} inicializado.")

def salvar_no_banco_historico(df):
    if df.empty:
        return
    inseridos = 0
    ignorados = 0
    with sqlite3.connect(NOME_DB_HISTORICO) as conn:
        for _, row in df.iterrows():
            try:
                colunas = row.index.tolist()
                placeholders = ', '.join(['?'] * len(colunas))
                col_names = ', '.join(colunas)
                sql = f"INSERT INTO jogos ({col_names}) VALUES ({placeholders})"
                conn.execute(sql, tuple(row))
                inseridos += 1
            except sqlite3.IntegrityError:
                ignorados += 1
        conn.commit()
    if ignorados > 0:
        log.info(f"Resultado: {inseridos} novos jogos salvos, {ignorados} já existiam e foram ignorados.")
    else:
        log.info(f"Sucesso: {inseridos} jogos salvos no banco histórico.")

def coletar_liga_historica(driver, url_liga, nome_liga_config, temporada_nome, rodada_inicial=0):
    log.info(f"--- Iniciando {nome_liga_config} | Temp: {temporada_nome} ---")
    driver.get(url_liga)
    wait = WebDriverWait(driver, 15)

    try:
        # 1. Clicar na aba "Jogos"
        tab_jogos = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="#matches"]')))
        driver.execute_script("arguments[0].click();", tab_jogos)
        time.sleep(2)

        # 2. Clicar em "Por semana de jogo" (By game week)
        # O seletor pode variar, vamos tentar um mais genérico se o nth-child falhar
        try:
            btn_semana = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Por semana de jogo')]")))
        except:
            btn_semana = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.xcfgSettings:nth-child(2)')))
        
        driver.execute_script("arguments[0].click();", btn_semana)
        time.sleep(2)

        # 3. Pegar todas as rodadas disponíveis no dropdown
        dropdown_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select.xcfgSettings.xcfgSettingsSel')))
        dropdown_rodada = Select(dropdown_elem)
        opcoes = dropdown_rodada.options
        total_rodadas = len(opcoes)
        log.info(f"Total de {total_rodadas} rodadas encontradas.")
        
        # Calcular o fim do loop com base no limite
        fim_loop = total_rodadas
        if LIMITE_RODADAS is not None:
            fim_loop = min(total_rodadas, rodada_inicial + LIMITE_RODADAS)
            log.info(f"Limite configurado: processando até a rodada {fim_loop}")

        for i in range(rodada_inicial, fim_loop):
            # Voltar para a página da liga se não estivermos nela (após raspar jogos individuais)
            if i > rodada_inicial:
                driver.get(url_liga)
                time.sleep(2)
                # Re-clicar em Jogos e Filtro
                tab_jogos = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="#matches"]')))
                driver.execute_script("arguments[0].click();", tab_jogos)
                time.sleep(1)
                try:
                    btn_semana = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Por semana de jogo')]")))
                except:
                    btn_semana = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.xcfgSettings:nth-child(2)')))
                driver.execute_script("arguments[0].click();", btn_semana)
                time.sleep(2)

            # 1. Garantir que a Temporada está correta
            try:
                # O site tem vários <select class="xcfgSettingsSel">. O de temporada contém anos (ex: 2025).
                dropdowns = driver.find_elements(By.CSS_SELECTOR, "select.xcfgSettingsSel")
                for dd_elem in dropdowns:
                    dd = Select(dd_elem)
                    try:
                        # Verifica se o dropdown atual é o de temporada (contém 2024, 2025 ou 2026)
                        opcoes_texto = " ".join([o.text for o in dd.options[:5]])
                        if "20" in opcoes_texto:
                            texto_selecionado = dd.first_selected_option.text
                            if temporada_nome not in texto_selecionado:
                                log.info(f"Temporada resetou para '{texto_selecionado}'. Re-selecionando {temporada_nome}...")
                                # ... resto da lógica ...
                            for opt in dd.options:
                                if temporada_nome in opt.text:
                                    dd.select_by_visible_text(opt.text)
                                    time.sleep(5)
                                    # Após mudar temporada, precisa re-clicar nas abas
                                    btn_jogos = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Jogos')]")))
                                    driver.execute_script("arguments[0].click();", btn_jogos)
                                    time.sleep(2)
                                    btn_semana = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Por semana de jogo')]")))
                                    driver.execute_script("arguments[0].click();", btn_semana)
                                    time.sleep(2)
                                    break
                            break
                    except:
                        continue
            except Exception as e:
                log.debug(f"Aviso ao verificar temporada: {e}")

            # 2. Re-localizar o dropdown de rodadas
            dropdown_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select.xcfgSettings.xcfgSettingsSel')))
            dropdown = Select(dropdown_elem)
            
            # Pega a opção pelo índice e extrai o valor interno do site
            opcao = dropdown.options[i]
            valor_opcao = opcao.get_attribute("value")
            rodada_texto = opcao.text
            
            log.info(f"-> Selecionando Opção Dropdown: {rodada_texto} (Índice {i}, Valor {valor_opcao})")
            
            # Forçar a seleção via JavaScript para disparar os eventos do site
            driver.execute_script("""
                var select = arguments[0];
                select.value = arguments[1];
                select.dispatchEvent(new Event('change', { bubbles: true }));
            """, dropdown_elem, valor_opcao)
            
            time.sleep(5) # Espera carregar os jogos da rodada (AJAX)

            # 3. Listar links dos jogos
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            links_jogos = []
            # Procura por links de jogos na tabela de dados
            for a in soup.select('table.table-data__table a[href*="/pt-br/match/"]'):
                href = a['href'].split('#')[0]
                if not href.startswith("http"):
                    href = "https://redscores.com" + href
                if href not in links_jogos:
                    links_jogos.append(href)
            
            log.info(f"Encontrados {len(links_jogos)} jogos carregados na tela.")

            dados_rodada = []
            for url_match in links_jogos:
                log.info(f"   Lendo jogo: {url_match}")
                # Passamos o texto do dropdown como fallback, mas o data.py vai priorizar o que ler na página
                res = dt.raspar_detalhes_confronto(driver, url_match, rodada_nome=f"Rodada {rodada_texto}")
                if res:
                    res['Liga'] = nome_liga_config
                    res['Temporada'] = temporada_nome
                    
                    # Preparar para o DataFrame (ajustando nomes de colunas do banco)
                    # O banco espera: Data, Home, Away, Liga, Temporada, Rodada, H_Gols_FT, ...
                    gols_ft = dt._converter_stat_para_int(res['Placar_FT'])
                    gols_ht = dt._converter_stat_para_int(res['Placar_HT'])
                    chutes = dt._converter_stat_para_int(res['Chutes'])
                    chutes_gol = dt._converter_stat_para_int(res['Chutes_Gol'])
                    ataques = dt._converter_stat_para_int(res['Ataques'])
                    escanteios = dt._converter_stat_para_int(res['Escanteios'])

                    row = {
                        "Data": res["Data"], "Home": res["Home"], "Away": res["Away"],
                        "Liga": res["Liga"], "Temporada": res["Temporada"], "Rodada": res["Rodada"],
                        "H_Gols_FT": gols_ft[0], "A_Gols_FT": gols_ft[1],
                        "H_Gols_HT": gols_ht[0], "A_Gols_HT": gols_ht[1],
                        "H_Chute": chutes[0], "A_Chute": chutes[1],
                        "H_Chute_Gol": chutes_gol[0], "A_Chute_Gol": chutes_gol[1],
                        "H_Ataques": ataques[0], "A_Ataques": ataques[1],
                        "H_Escanteios": escanteios[0], "A_Escanteios": escanteios[1],
                        "Odd_H": res["Odd_H"], "Odd_D": res["Odd_D"], "Odd_A": res["Odd_A"]
                    }
                    dados_rodada.append(row)
                    time.sleep(random.uniform(1, 2)) # Pausa entre jogos

            if dados_rodada:
                df = pd.DataFrame(dados_rodada)
                salvar_no_banco_historico(df)
            
            
            # Pausa maior entre rodadas
            time.sleep(random.uniform(3, 5))

    except Exception as e:
        log.error(f"Erro crítico na coleta da liga {nome_liga_config}: {e}")

def main():
    inicializar_banco_historico()
    driver = None
    try:
        log.info("Iniciando driver...")
        driver = login_redscore(REDSCORE_USER, REDSCORE_PASS)
        
        # Teste piloto com Brasileirão 2025
        liga_nome = "Brasil - Serie A"
        config = cfg.LIGAS_HISTORICO[liga_nome]
        
        # Vamos rodar apenas a primeira rodada como teste
        url_2025 = config["temporadas"]["2025"]
        coletar_liga_historica(driver, url_2025, liga_nome, "2025", rodada_inicial=0)
        
    finally:
        if driver:
            driver.quit()
            log.info("Driver encerrado.")

if __name__ == "__main__":
    main()
