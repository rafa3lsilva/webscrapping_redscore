#!/usr/bin/env python3
"""
RedScore — Ponto de entrada principal.

Uso:
    python main.py              # Executa a rotina diária completa
    python main.py --migrate    # Executa apenas a migração para Supabase
"""
import sys
import os

# Garante que o diretório do projeto é o CWD (para paths relativos funcionarem)
os.chdir(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    if "--migrate" in sys.argv:
        # Executa apenas a migração
        from database import migrate  # noqa: F401 — executa como script
    else:
        # Executa a rotina diária completa
        from collector.redscore import rotina_diaria_noturna
        rotina_diaria_noturna()
