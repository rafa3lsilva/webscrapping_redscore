# Projeto: Coletor de Dados Históricos Flashscore

Este documento detalha o planejamento técnico para a criação do sistema de web scraping focado em dados históricos do Flashscore, utilizando como base inicial o **Brasileirão Betano 2025**.

## 1. Objetivo da Fase 1
Coletar a lista completa de resultados de uma temporada específica, extraindo informações básicas de cada partida para construir uma base de dados de referência (Match IDs).

### Dados a serem coletados:
- **Match ID**: Identificador único da partida (Ex: `g_1_XXXXXXXX`).
- **Data/Hora**: Momento da realização do jogo.
- **Time Casa**: Nome do clube mandante.
- **Time Fora**: Nome do clube visitante.
- **Placar**: Gols Casa e Gols Fora.

---

## 2. Especificações Técnicas (Seletores)

Com base na análise da página de resultados (`/resultados/`), os seguintes seletores foram validados:

### Seletores CSS
| Elemento | Seletor CSS | Descrição |
| :--- | :--- | :--- |
| **Linha da Partida** | `.event__match` | Bloco principal que contém os dados de um jogo. |
| **Data/Hora** | `.event__time` | Texto contendo o timestamp do jogo. |
| **Time Casa** | `.event__homeParticipant` | Nome do time mandante. |
| **Time Fora** | `.event__awayParticipant` | Nome do time visitante. |
| **Gols Casa** | `.event__score--home` | Placar final do mandante. |
| **Gols Fora** | `.event__score--away` | Placar final do visitante. |
| **Botão "Mais"** | `.event__more` | Link para carregar jogos anteriores (histórico). |

### XPaths (Alternativa Robusta)
| Elemento | XPath |
| :--- | :--- |
| **Match Row** | `//div[contains(@class, 'event__match')]` |
| **Match ID** | `./@id` (Extrair via atributo no loop) |
| **Botão Mostrar Mais** | `//a[contains(@class, 'event__more')]` |

---

## 3. Fluxo de Execução (Algoritmo)

Para garantir a coleta de todos os dados históricos, o script deve seguir este fluxo:

1. **Navegação**: Acessar a URL da liga (Ex: `https://www.flashscore.com.br/futebol/brasil/brasileirao-betano-2025/resultados/`).
2. **Carregamento Completo**:
   - Verificar se o botão "Mostrar mais jogos" está visível.
   - Enquanto visível: Clicar no botão e aguardar o carregamento de novos elementos.
   - *Nota*: Em temporadas muito longas, pode ser necessário rolar a página para acionar o lazy loading.
3. **Extração**:
   - Localizar todos os elementos `.event__match`.
   - Iterar sobre cada elemento para extrair ID, Times e Placar.
   - **Tratamento de ID**: O atributo `id` geralmente vem como `g_1_ABC123`. Devemos limpar para obter apenas `ABC123`.
4. **Armazenamento**:
   - Salvar em formato CSV para portabilidade.
   - (Opcional) Salvar em SQLite para evitar duplicatas em coletas futuras.

---

## 4. Estrutura Sugerida do Projeto

```text
webscrapping_redscore/
├── historical/
│   ├── configs/
│   │   └── ligas.json           # URLs das ligas para histórico
│   ├── data/
│   │   └── resultados_br_2025.csv
│   ├── logs/
│   ├── utils/
│   │   └── parser.py            # Limpeza de strings e datas
│   └── scraper_historico.py     # Script principal da Fase 1
├── seletores/
│   └── seletor.py               # Seletores detalhados (já existente)
└── requirements.txt
```

---

## 5. Próximos Passos (Fase 2)
Após obter os Match IDs da Fase 1, o sistema poderá:
1. Iterar sobre cada ID para acessar `flashscore.com.br/jogo/ID/estatisticas`.
2. Usar a lógica já desenvolvida no `seletores/seletor.py` para extrair xG, xGOT e outras métricas avançadas.
3. Unificar os dados históricos básicos com as estatísticas detalhadas.

---

> [!IMPORTANT]
> O Flashscore utiliza proteção contra bots (Cloudflare/DataDome). Recomenda-se o uso de **Playwright** com o plugin **Stealth** ou a utilização de um perfil de usuário real para evitar bloqueios durante o carregamento de grandes volumes de dados históricos.
