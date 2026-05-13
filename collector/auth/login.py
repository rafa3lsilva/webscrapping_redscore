from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import os
import shutil
import subprocess
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager


def limpar_perfil_firefox(caminho_perfil):
    """
    Força o encerramento de instâncias antigas do Firefox e
    apaga a pasta de perfil, criando uma nova.
    """
    if os.path.exists(caminho_perfil):
        print(f"🔄 Encerrando possíveis instâncias do Firefox...")
        try:
            subprocess.run(["pkill", "-f", "firefox"], check=False)
        except Exception as e:
            print(f"⚠️ Não foi possível finalizar processos do Firefox: {e}")

        print(f"🧹 Limpando perfil antigo: {caminho_perfil}")
        shutil.rmtree(caminho_perfil, ignore_errors=True)

    os.makedirs(caminho_perfil, exist_ok=True)
    print(f"📁 Novo perfil criado: {caminho_perfil}")


def iniciar_driver_global():
    """
    Inicializa e retorna uma instância única do WebDriver para Firefox,
    garantindo um perfil limpo a cada execução.
    """
    print("🔧 Configurando WebDriver para Firefox...")
    firefox_options = Options()

    caminho_perfil = os.path.join(os.getcwd(), 'firefox_profile')
    limpar_perfil_firefox(caminho_perfil)

    firefox_options.add_argument("-profile")
    firefox_options.add_argument(caminho_perfil)

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"
    firefox_options.set_preference("general.useragent.override", user_agent)
    firefox_options.set_preference("dom.webdriver.enabled", False)
    firefox_options.set_preference('useAutomationExtension', False)
    firefox_options.add_argument("--headless")  # Descomente em produção

    try:
        caminho_driver_local = "./geckodriver"
        if os.path.exists(caminho_driver_local):
            servico = FirefoxService(executable_path=caminho_driver_local)
        else:
            instalador = GeckoDriverManager().install()
            servico = FirefoxService(executable_path=instalador)

        driver = webdriver.Firefox(service=servico, options=firefox_options)
        print("✅ Navegador iniciado com sucesso.")
        return driver
    except Exception as e:
        print(f"❌ Erro ao iniciar o WebDriver: {e}")
        return None


def login_redscore(user, password):
    """
    Faz login no RedScore usando o WebDriver e retorna o driver autenticado.
    """
    print("🔧 Iniciando sessão no RedScore...")
    driver = iniciar_driver_global()
    if driver is None:
        return None

    print("🌐 Acedendo ao site...")
    driver.get("https://redscores.com/pt-br/user/login")

    # Aceita cookies
    try:
        botao_aceitar_cookies = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "div.cookieinfo-close"))
        )
        botao_aceitar_cookies.click()
        print("✅ Banner de cookies aceite.")
    except Exception:
        print("ℹ️ Banner de cookies não encontrado, a continuar...")

    # Fecha o banner de publicidade que bloqueia a tela
    try:
        # Identifica tanto o botão de fechar quanto o contêiner principal do banner
        close_button_id = "clever-86072-1911608-sticky-footer-stickyfooter-close"
        banner_container_id = "clever-86072-1911608-sticky-footer-shadow"
        
        wait = WebDriverWait(driver, 5)
        
        print("🔎 Procurando pelo banner de publicidade...")
        botao_fechar_banner = wait.until(
            EC.element_to_be_clickable((By.ID, close_button_id))
        )
        
        # Localiza o elemento do contêiner ANTES de clicar em fechar
        container_do_banner = driver.find_element(By.ID, banner_container_id)
        
        botao_fechar_banner.click()
        print("✅ Banner fechado. Aguardando o seu desaparecimento...")
        
        # --- A ESPERA INTELIGENTE ---
        # Agora, espera até 10 segundos para que o contêiner do banner "desapareça" da página.
        # Isso lida com qualquer animação de "fade out" ou "slide out".
        WebDriverWait(driver, 10).until(
            EC.staleness_of(container_do_banner)
        )
        print("✅ Animação do banner concluída. O caminho está livre.")
        
    except TimeoutException:
        print("ℹ️ Banner de publicidade não foi encontrado ou já havia desaparecido. Prosseguindo...")
    except Exception as e:
        print(f"⚠️ Ocorreu um erro inesperado ao lidar com o banner: {e}")

    # Abre modal de login
    try:
        btn_login = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href,'login')]"))
        )
        driver.execute_script("arguments[0].click();", btn_login)
        print("✅ Clique no botão de login executado via JavaScript.")
    except Exception as e:
        print("❌ Não foi possível encontrar ou clicar no botão de login.")
        driver.quit()
        raise e

    # Preenche formulário e envia
    try:
        campo_email = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "email"))
        )
        campo_email.send_keys(user)
        driver.find_element(By.NAME, "password").send_keys(password)

        botao_entrar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'button[name="loginSend"]'))
        )
        botao_entrar.click()
    except Exception as e:
        print("❌ Não foi possível preencher os dados ou clicar no botão 'Entrar'.")
        driver.quit()
        raise e

    time.sleep(5)
    print("✅ Login efetuado com sucesso")
    return driver
