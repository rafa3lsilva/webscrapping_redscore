# Coletor Histórico e Padronização de Dados

Este plano visa implementar um novo motor de coleta para buscar dados históricos detalhados do RedScore, organizando-os por Temporada e Rodada, e preparar a transição para um banco de dados mais robusto.

## User Review Required

> [!IMPORTANT]
> O coletor histórico visitará uma página por jogo. Para uma temporada de 380 jogos (ex: Brasileirão), isso significa 380 acessos. Recomendamos rodar este script em horários de baixo tráfego para evitar bloqueios por IP.

## Proposed Changes

### [Component: Coletor Histórico]

#### [NEW] [coletor_historico.py](file:///home/rafael/Documentos/webscrapping_redscore/coletor_historico.py)
*   Criação do script principal para navegação em Ligas -> Temporadas -> Rodadas.
*   Inicialização do banco `dados_historicos.db`.
*   Lógica de navegação via Selenium para manipular os dropdowns de Rodada.

#### [MODIFY] [data.py](file:///home/rafael/Documentos/webscrapping_redscore/data.py)
*   Adição de uma função `raspar_detalhes_confronto(driver, url)` para extrair estatísticas (chutes, cantos, etc.) diretamente da página do jogo, já que a página da liga só mostra o placar.

### [Component: Configuração]

#### [MODIFY] [ligas_config.py](file:///home/rafael/Documentos/webscrapping_redscore/ligas_config.py)
*   Adição de um dicionário `LIGAS_HISTORICO` contendo os links base de cada liga para facilitar a automação.

## Verification Plan

### Automated Tests
1.  **Teste de Navegação:** Rodar o script para apenas 1 rodada e verificar se ele consegue selecionar o dropdown e listar os links dos jogos.
2.  **Teste de Raspagem:** Verificar se as estatísticas de um jogo finalizado (ex: Chutes a gol) estão sendo salvas corretamente no `dados_historicos.db`.

### Manual Verification
1.  Abrir o `dados_historicos.db` com um visualizador SQLite e conferir se as colunas `Temporada` e `Rodada` estão preenchidas corretamente conforme o site.
