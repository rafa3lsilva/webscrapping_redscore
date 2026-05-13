import os
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

REDSCORE_USER = os.getenv("REDSCORE_USER")
REDSCORE_PASS = os.getenv("REDSCORE_PASS")

if not REDSCORE_USER or not REDSCORE_PASS:
    raise ValueError("⚠️ Usuário e senha do RedScore não encontrados no .env")
