import os
import sys
import logging
import getpass
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

log = logging.getLogger(__name__)

def obter_webdriver_chrome():
    """
    Inicializa o WebDriver para Chrome de forma extremamente robusta,
    ajustando automaticamente as variáveis de ambiente necessárias para
    rodar tanto localmente quanto em servidores/cron (resolvendo problemas com snap/confinamento).
    """
    # 1. Configurar variáveis de ambiente cruciais para o snap e cron no Linux
    # Garante que HOME esteja definido (essencial para Chromium Snap no Ubuntu)
    if "HOME" not in os.environ:
        user = getpass.getuser()
        os.environ["HOME"] = f"/home/{user}"
        log.info(f"🔧 Variável HOME não estava definida. Definida temporariamente como: {os.environ['HOME']}")

    # Garante que PATH contenha caminhos comuns do Linux, especialmente /snap/bin e /usr/local/bin
    caminhos_adicionais = ["/snap/bin", "/usr/local/bin", "/usr/bin", "/bin"]
    path_atual = os.environ.get("PATH", "")
    for caminho in caminhos_adicionais:
        if caminho not in path_atual:
            path_atual = f"{path_atual}:{caminho}" if path_atual else caminho
    os.environ["PATH"] = path_atual
    log.debug(f"🔧 PATH configurado: {os.environ['PATH']}")

    # 2. Configurar opções padrão para execução estável e headless
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # 3. Adicionar um User-Agent moderno para evitar bloqueios
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # 4. Estratégias de Inicialização em Cascata
    
    # Estratégia A: Inicialização nativa do Selenium 4.x (recomenda-se usar o Selenium Manager embutido)
    try:
        log.info("🤖 Tentando inicializar o Chrome WebDriver via Selenium Manager...")
        driver = webdriver.Chrome(options=options)
        log.info("✅ Chrome WebDriver iniciado com sucesso usando Selenium Manager padrão!")
        return driver
    except Exception as e_std:
        log.warning(f"⚠️ Falha na inicialização padrão do Selenium Manager: {e_std}")

    # Estratégia B: Procurar por binários comuns do Chromium instalados via snap/apt e definir explicitamente
    browser_binaries = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
        "/snap/bin/google-chrome",
    ]
    
    found_binary = None
    for binary in browser_binaries:
        if os.path.exists(binary):
            found_binary = binary
            break
            
    if found_binary:
        log.info(f"🔎 Encontrado binário do navegador em: {found_binary}. Testando com ele...")
        try:
            options.binary_location = found_binary
            driver = webdriver.Chrome(options=options)
            log.info(f"✅ WebDriver iniciado com sucesso usando binário {found_binary}!")
            return driver
        except Exception as e_bin:
            log.warning(f"⚠️ Falha ao inicializar com binário {found_binary}: {e_bin}")
            # Limpa para tentar a próxima estratégia (bypassando o validador do setter do Selenium)
            options._binary_location = None

    # Estratégia C: Tentar usar drivers do sistema explicitamente via Service
    chromedriver_paths = [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/local/bin/chromedriver",
        "/snap/bin/chromium.chromedriver",
    ]
    
    for driver_path in chromedriver_paths:
        if os.path.exists(driver_path):
            log.info(f"🔎 Encontrado chromedriver em: {driver_path}. Tentando com Service...")
            try:
                service = Service(executable_path=driver_path)
                driver = webdriver.Chrome(service=service, options=options)
                log.info(f"✅ WebDriver iniciado com sucesso usando chromedriver em {driver_path}!")
                return driver
            except Exception as e_srv:
                log.warning(f"⚠️ Falha ao inicializar com chromedriver em {driver_path}: {e_srv}")

    # Estratégia D: Fallback para o webdriver-manager
    try:
        log.info("🤖 Tentando inicializar usando ChromeDriverManager...")
        from webdriver_manager.chrome import ChromeDriverManager
        driver_path = ChromeDriverManager().install()
        driver = webdriver.Chrome(service=Service(driver_path), options=options)
        log.info("✅ WebDriver iniciado com sucesso usando ChromeDriverManager!")
        return driver
    except Exception as e_wdm:
        log.error(f"❌ Falha crítica ao inicializar o Chrome WebDriver com todas as estratégias: {e_wdm}")
        raise e_wdm
