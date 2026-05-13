# 🧪 Laboratório — Projetos em Desenvolvimento

Este diretório contém pipelines experimentais que **não fazem parte da produção**.

> ⚠️ Nenhum destes scripts é chamado pelo `executar_scraping.sh` ou pelo `main.py`.

## Status dos Experimentos

| Pasta | Descrição | Maturidade | Próximo Passo |
|---|---|---|---|
| `flashscore/` | Coleta via API GraphQL (xG, odds open/close) | 🟡 70% | Expandir para 27 ligas, desligar TEST_MODE |
| `bottom_up/` | Coleta detalhada via RedScore (62 métricas) | 🟡 60% | Resolver paths, testar com múltiplas ligas |
| `historical/` | Scraper Flashscore Playwright (async) | 🟠 40% | Desligar TEST_LIMIT, expandir ligas |
| `seletores/` | Lab de testes de seletores CSS/Flashscore | 🔴 Protótipo | Integrar ao flashscore/ |
| `analysis/` | Dashboard xG e análise de ligas | 🟢 Completo | Manter como referência |
| `utils/` | Utilitários diversos | 🔴 Mínimo | Expandir conforme necessário |

## Como Executar um Experimento

```bash
# Sempre execute a partir da raiz do projeto para os imports funcionarem:
cd /home/rafael/Documentos/webscrapping_redscore
python lab/flashscore/coletor_flashscore.py
```

## Regra de Promoção

Quando um experimento estiver maduro:
1. Criar testes em `tests/`
2. Validar com dados reais
3. Mover para `collector/` ou `database/`
4. Atualizar `main.py` e `executar_scraping.sh`
