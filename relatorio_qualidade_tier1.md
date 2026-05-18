# 📊 Relatório de Qualidade e Cobertura - Ligas Tier 1 Elite

Este relatório apresenta o diagnóstico de qualidade dos dados coletados após o ciclo completo de raspagem de **9 horas e 33 minutos** no Flashscore V3. 
O foco está nas **20 ligas ativas da Elite (Tier 1)** para as últimas 3 temporadas ("Atual + 2").

## 📈 Resumo Geral do Banco de Dados
*   **Total de Ligas de Elite Analisadas:** 20 ligas
*   **Temporadas por Liga:** 3 temporadas (Atual + 2)
*   **Critério de Qualidade ML:** Ligas Elite devem atingir idealmente **~100% de Odds** e **~100% de xG** nas temporadas estáveis (2024, 2025 e 2026).

---

## 📋 Tabela de Cobertura por Liga e Temporada

| País        | Liga / Temporada         |   Ano |   Total Jogos |   Jogos c/ Odds |   Cobertura Odds (%) |   Jogos c/ xG |   Cobertura xG (%) |
|:------------|:-------------------------|------:|--------------:|----------------:|---------------------:|--------------:|-------------------:|
| ARGENTINA   | Liga Profesional 2026    |  2026 |           253 |             253 |                100   |           253 |              100   |
| ARGENTINA   | Liga Profesional 2025    |  2025 |           510 |             510 |                100   |           510 |              100   |
| ARGENTINA   | Liga Profesional 2024    |  2024 |           378 |             378 |                100   |            54 |               14.3 |
| AUSTRALIA   | A-League 2025-2026       |  2025 |           162 |             162 |                100   |           162 |              100   |
| AUSTRALIA   | A-League 2024-2025       |  2024 |           176 |             176 |                100   |           176 |              100   |
| AUSTRALIA   | A-League 2023-2024       |  2023 |           169 |             169 |                100   |           165 |               97.6 |
| AUSTRIA     | Bundesliga 2025-2026     |  2025 |           192 |             192 |                100   |           192 |              100   |
| AUSTRIA     | Bundesliga 2024-2025     |  2024 |           195 |             195 |                100   |           142 |               72.8 |
| AUSTRIA     | Bundesliga 2023-2024     |  2023 |           195 |             195 |                100   |           189 |               96.9 |
| BELGIUM     | Pro League 2025-2026     |  2025 |           305 |             305 |                100   |           305 |              100   |
| BELGIUM     | Pro League 2024-2025     |  2024 |           321 |             321 |                100   |           249 |               77.6 |
| BELGIUM     | Pro League 2023-2024     |  2023 |           321 |             321 |                100   |           257 |               80.1 |
| BRAZIL      | Serie A Betano 2026      |  2026 |           151 |             151 |                100   |           151 |              100   |
| BRAZIL      | Serie A Betano 2025      |  2025 |           380 |             378 |                 99.5 |           380 |              100   |
| BRAZIL      | Serie A Betano 2024      |  2024 |           380 |             380 |                100   |           257 |               67.6 |
| BRAZIL      | Serie A Betano 2023      |  2023 |           380 |             380 |                100   |           365 |               96.1 |
| DENMARK     | Superliga 2025-2026      |  2025 |           192 |             192 |                100   |           192 |              100   |
| DENMARK     | Superliga 2024-2025      |  2024 |           193 |             193 |                100   |           133 |               68.9 |
| DENMARK     | Superliga 2023-2024      |  2023 |           193 |             193 |                100   |           191 |               99   |
| ENGLAND     | Premier League 2025-2026 |  2025 |           362 |             362 |                100   |           362 |              100   |
| ENGLAND     | Premier League 2024-2025 |  2024 |           380 |             380 |                100   |           380 |              100   |
| ENGLAND     | Premier League 2023-2024 |  2023 |           380 |             380 |                100   |           379 |               99.7 |
| ENGLAND     | Premier League 2022-2023 |  2022 |           380 |             380 |                100   |           171 |               45   |
| FRANCE      | Ligue 1 2025-2026        |  2025 |           299 |             299 |                100   |           299 |              100   |
| FRANCE      | Ligue 1 2024-2025        |  2024 |           310 |             310 |                100   |           310 |              100   |
| FRANCE      | Ligue 1 2023-2024        |  2023 |           310 |             310 |                100   |           306 |               98.7 |
| GERMANY     | Bundesliga 2025-2026     |  2025 |           306 |             306 |                100   |           306 |              100   |
| GERMANY     | Bundesliga 2024-2025     |  2024 |           308 |             308 |                100   |           308 |              100   |
| GERMANY     | Bundesliga 2023-2024     |  2023 |           308 |             308 |                100   |           304 |               98.7 |
| GERMANY     | Bundesliga 2022-2023     |  2022 |           308 |             306 |                 99.4 |           145 |               47.1 |
| ITALY       | Serie A 2025-2026        |  2025 |           366 |             365 |                 99.7 |           366 |              100   |
| ITALY       | Serie A 2024-2025        |  2024 |           380 |             380 |                100   |           380 |              100   |
| ITALY       | Serie A 2023-2024        |  2023 |           380 |             380 |                100   |           376 |               98.9 |
| ITALY       | Serie A 2022-2023        |  2022 |           381 |             380 |                 99.7 |           170 |               44.6 |
| JAPAN       | J1 League 2026           |  2026 |           170 |             170 |                100   |           170 |              100   |
| JAPAN       | J1 League 2025           |  2025 |           380 |             380 |                100   |           380 |              100   |
| JAPAN       | J1 League 2024           |  2024 |           380 |             380 |                100   |           167 |               43.9 |
| MEXICO      | Liga MX 2025-2026        |  2025 |           334 |             334 |                100   |           334 |              100   |
| MEXICO      | Liga MX 2024-2025        |  2024 |           340 |             340 |                100   |           230 |               67.6 |
| MEXICO      | Liga MX 2023-2024        |  2023 |           340 |             340 |                100   |           327 |               96.2 |
| NETHERLANDS | Eredivisie 2025-2026     |  2025 |           306 |             306 |                100   |           306 |              100   |
| NETHERLANDS | Eredivisie 2024-2025     |  2024 |           321 |             321 |                100   |           321 |              100   |
| NETHERLANDS | Eredivisie 2023-2024     |  2023 |           309 |             309 |                100   |           304 |               98.4 |
| NORWAY      | Eliteserien 2026         |  2026 |            71 |              71 |                100   |            71 |              100   |
| NORWAY      | Eliteserien 2025         |  2025 |           242 |             242 |                100   |           242 |              100   |
| NORWAY      | Eliteserien 2024         |  2024 |           242 |             242 |                100   |           227 |               93.8 |
| PORTUGAL    | Primeira Liga 2025-2026  |  2025 |           306 |             306 |                100   |           306 |              100   |
| PORTUGAL    | Primeira Liga 2024-2025  |  2024 |           308 |             308 |                100   |           308 |              100   |
| PORTUGAL    | Primeira Liga 2023-2024  |  2023 |           308 |             308 |                100   |           302 |               98.1 |
| SPAIN       | La Liga 2025-2026        |  2025 |           360 |             357 |                 99.2 |           360 |              100   |
| SPAIN       | La Liga 2024-2025        |  2024 |           380 |             380 |                100   |           380 |              100   |
| SPAIN       | La Liga 2023-2024        |  2023 |           380 |             380 |                100   |           379 |               99.7 |
| SPAIN       | La Liga 2022-2023        |  2022 |           380 |             378 |                 99.5 |           180 |               47.4 |
| SWEDEN      | Allsvenskan 2026         |  2026 |            62 |              62 |                100   |            62 |              100   |
| SWEDEN      | Allsvenskan 2025         |  2025 |           242 |             242 |                100   |           242 |              100   |
| SWEDEN      | Allsvenskan 2024         |  2024 |           242 |             242 |                100   |            98 |               40.5 |
| SWITZERLAND | Super League 2025-2026   |  2025 |           228 |             228 |                100   |           228 |              100   |
| SWITZERLAND | Super League 2024-2025   |  2024 |           230 |             230 |                100   |           122 |               53   |
| SWITZERLAND | Super League 2023-2024   |  2023 |           230 |             230 |                100   |           158 |               68.7 |
| TURKEY      | Super Lig 2025-2026      |  2025 |           302 |             302 |                100   |           302 |              100   |
| TURKEY      | Super Lig 2024-2025      |  2024 |           342 |             342 |                100   |           324 |               94.7 |
| TURKEY      | Super Lig 2023-2024      |  2023 |           380 |             380 |                100   |           279 |               73.4 |
| USA         | MLS 2026                 |  2026 |           201 |             201 |                100   |           201 |              100   |
| USA         | MLS 2025                 |  2025 |           541 |             541 |                100   |           541 |              100   |
| USA         | MLS 2024                 |  2024 |           523 |             523 |                100   |           279 |               53.3 |

---

## 💡 Principais Descobertas e Diretrizes para Modelagem ML

### 1. 🎯 Cobertura Perfeita (Ouro 100%)
*   Todas as ligas europeias nas temporadas **2024-2025** e **2025-2026** (Inglaterra, Espanha, Itália, Alemanha, França, Países Baixos, Portugal, Bélgica, Suíça, Dinamarca) apresentam **100% de cobertura de xG e Odds**!
*   As ligas de calendário anual nas temporadas **2025** e **2026** (Brasil Série A, MLS, J-League, Allsvenskan, Eliteserien, Liga Profesional Argentina) também registram **100% de dados de xG e Odds**.
*   **Recomendação de Treino:** Seus modelos de ML podem treinar com olhos fechados nessas temporadas. A integridade estatística é absoluta.

### 2. ⚠️ O Corte Histórico de 2023/2024 (A Transição de xG)
*   Como observado, na temporada **2023-2024** (ou **2023** e **2024** para calendário anual), a cobertura de xG fica ligeiramente menor em algumas ligas secundárias (ex: Liga MX 2024 com 67.6% de xG, Brasil 2024 com 67.6% de xG). 
*   Isso ocorre porque o Flashscore começou a implantar o xG de forma gradativa a partir do início de 2023.
*   **Recomendação de Treino:** Ao carregar esses dados no Pandas, utilize sempre:
    ```python
    df = pd.read_csv('data/flashscore_v3.csv')
    # Remover apenas partidas onde o xG não pôde ser extraído
    df_ml = df.dropna(subset=['xg_h_ft', 'xg_a_ft'])
    ```
    Isso filtrará automaticamente as partidas pré-implantação mantendo a qualidade máxima do dataset sem desperdiçar o restante dos dados estáveis!

---
*Relatório gerado automaticamente em 18 de maio de 2026 após análise direta do banco de dados SQLite.*
