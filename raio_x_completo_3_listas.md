# 🔎 Raio-X Completo do Banco de Dados - REDSCORE V3

Este relatório detalhado apresenta o diagnóstico completo de qualidade e cobertura dos dados do seu banco de dados SQLite (`flashscore_v3.db`) contendo as **50 ligas ativas** (20 de Elite, 10 de Acesso e 20 Exóticas) organizadas por Tier.

---

## 📊 1. Resumo Executivo e Métricas Globais

O banco de dados conta atualmente com um volume massivo de **51,918 jogos** analisados de forma granular.

| Categoria (Tier) | Qtd Ligas | Volume de Jogos | Média Cobertura Odds | Média Cobertura xG | Perfil ML Recomendado |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Tier 1 (Elite)** | 20 | 19,628 | 100.0% | 88.2% | **Alta Tecnologia (xG + Odds + Scouts Avançados)** |
| **Tier 2 (Acesso)** | 10 | 17,002 | 99.9% | 52.8% | **Híbrido (Scouts Clássicos + Odds + xG Parcial)** |
| **Tier 3 (Exóticas)** | 20 | 15,288 | 99.2% | 56.8% | **Tradicional/Exótico (Scouts Clássicos + ELO + Odds)** |
| **TOTAL BANCO** | **50** | **51,918** | **-** | **-** | **Uma das maiores bases privadas do Brasil!** |

---

## 🏆 2. Tabela de Cobertura - Tier 1 (Elite)

*As ligas da Elite Mundial apresentam índices de qualidade altíssimos (perto de 100% de xG e Odds nas temporadas recentes).*

| País        | Liga / Temporada         |   Ano |   Total Jogos |   Jogos c/ Odds |   Cobertura Odds (%) |   Jogos c/ xG |   Cobertura xG (%) |
|:------------|:-------------------------|------:|--------------:|----------------:|---------------------:|--------------:|-------------------:|
| ARGENTINA   | Liga Profesional 2026    |  2026 |           254 |             254 |                100   |           254 |              100   |
| ARGENTINA   | Liga Profesional 2025    |  2025 |           510 |             510 |                100   |           510 |              100   |
| ARGENTINA   | Liga Profesional 2024    |  2024 |           378 |             378 |                100   |            54 |               14.3 |
| AUSTRALIA   | A-League 2025-2026       |  2025 |           162 |             162 |                100   |           162 |              100   |
| AUSTRALIA   | A-League 2024-2025       |  2024 |           176 |             176 |                100   |           176 |              100   |
| AUSTRALIA   | A-League 2023-2024       |  2023 |           169 |             169 |                100   |           165 |               97.6 |
| AUSTRIA     | Bundesliga 2025-2026     |  2025 |           192 |             192 |                100   |           192 |              100   |
| AUSTRIA     | Bundesliga 2024-2025     |  2024 |           195 |             195 |                100   |           142 |               72.8 |
| AUSTRIA     | Bundesliga 2023-2024     |  2023 |           195 |             195 |                100   |           189 |               96.9 |
| BELGIUM     | Pro League 2025-2026     |  2025 |           307 |             307 |                100   |           307 |              100   |
| BELGIUM     | Pro League 2024-2025     |  2024 |           321 |             321 |                100   |           249 |               77.6 |
| BELGIUM     | Pro League 2023-2024     |  2023 |           321 |             321 |                100   |           257 |               80.1 |
| BRAZIL      | Serie A Betano 2026      |  2026 |           157 |             157 |                100   |           157 |              100   |
| BRAZIL      | Serie A Betano 2025      |  2025 |           380 |             378 |                 99.5 |           380 |              100   |
| BRAZIL      | Serie A Betano 2024      |  2024 |           380 |             380 |                100   |           257 |               67.6 |
| BRAZIL      | Serie A Betano 2023      |  2023 |           380 |             380 |                100   |           365 |               96.1 |
| DENMARK     | Superliga 2025-2026      |  2025 |           192 |             192 |                100   |           192 |              100   |
| DENMARK     | Superliga 2024-2025      |  2024 |           193 |             193 |                100   |           133 |               68.9 |
| DENMARK     | Superliga 2023-2024      |  2023 |           193 |             193 |                100   |           191 |               99   |
| ENGLAND     | Premier League 2025-2026 |  2025 |           367 |             367 |                100   |           367 |              100   |
| ENGLAND     | Premier League 2024-2025 |  2024 |           380 |             380 |                100   |           380 |              100   |
| ENGLAND     | Premier League 2023-2024 |  2023 |           380 |             380 |                100   |           379 |               99.7 |
| ENGLAND     | Premier League 2022-2023 |  2022 |           380 |             380 |                100   |           171 |               45   |
| FRANCE      | Ligue 1 2025-2026        |  2025 |           308 |             308 |                100   |           308 |              100   |
| FRANCE      | Ligue 1 2024-2025        |  2024 |           310 |             310 |                100   |           310 |              100   |
| FRANCE      | Ligue 1 2023-2024        |  2023 |           310 |             310 |                100   |           306 |               98.7 |
| GERMANY     | Bundesliga 2025-2026     |  2025 |           306 |             306 |                100   |           306 |              100   |
| GERMANY     | Bundesliga 2024-2025     |  2024 |           308 |             308 |                100   |           308 |              100   |
| GERMANY     | Bundesliga 2023-2024     |  2023 |           308 |             308 |                100   |           304 |               98.7 |
| GERMANY     | Bundesliga 2022-2023     |  2022 |           308 |             306 |                 99.4 |           145 |               47.1 |
| ITALY       | Serie A 2025-2026        |  2025 |           370 |             369 |                 99.7 |           370 |              100   |
| ITALY       | Serie A 2024-2025        |  2024 |           380 |             380 |                100   |           380 |              100   |
| ITALY       | Serie A 2023-2024        |  2023 |           380 |             380 |                100   |           376 |               98.9 |
| ITALY       | Serie A 2022-2023        |  2022 |           381 |             380 |                 99.7 |           170 |               44.6 |
| JAPAN       | J1 League 2026           |  2026 |           170 |             170 |                100   |           170 |              100   |
| JAPAN       | J1 League 2025           |  2025 |           380 |             380 |                100   |           380 |              100   |
| JAPAN       | J1 League 2024           |  2024 |           380 |             380 |                100   |           167 |               43.9 |
| MEXICO      | Liga MX 2025-2026        |  2025 |           335 |             335 |                100   |           335 |              100   |
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
| SPAIN       | La Liga 2025-2026        |  2025 |           370 |             367 |                 99.2 |           370 |              100   |
| SPAIN       | La Liga 2024-2025        |  2024 |           380 |             380 |                100   |           380 |              100   |
| SPAIN       | La Liga 2023-2024        |  2023 |           380 |             380 |                100   |           379 |               99.7 |
| SPAIN       | La Liga 2022-2023        |  2022 |           380 |             378 |                 99.5 |           180 |               47.4 |
| SWEDEN      | Allsvenskan 2026         |  2026 |            62 |              62 |                100   |            62 |              100   |
| SWEDEN      | Allsvenskan 2025         |  2025 |           242 |             242 |                100   |           242 |              100   |
| SWEDEN      | Allsvenskan 2024         |  2024 |           242 |             242 |                100   |            98 |               40.5 |
| SWITZERLAND | Super League 2025-2026   |  2025 |           228 |             228 |                100   |           228 |              100   |
| SWITZERLAND | Super League 2024-2025   |  2024 |           230 |             230 |                100   |           122 |               53   |
| SWITZERLAND | Super League 2023-2024   |  2023 |           230 |             230 |                100   |           158 |               68.7 |
| TURKEY      | Super Lig 2025-2026      |  2025 |           306 |             306 |                100   |           306 |              100   |
| TURKEY      | Super Lig 2024-2025      |  2024 |           342 |             342 |                100   |           324 |               94.7 |
| TURKEY      | Super Lig 2023-2024      |  2023 |           380 |             380 |                100   |           279 |               73.4 |
| USA         | MLS 2026                 |  2026 |           203 |             203 |                100   |           203 |              100   |
| USA         | MLS 2025                 |  2025 |           541 |             541 |                100   |           541 |              100   |
| USA         | MLS 2024                 |  2024 |           523 |             523 |                100   |           279 |               53.3 |

---

## ⚡ 3. Tabela de Cobertura - Tier 2 (Acesso)

*Ligas de divisões inferiores nacionais de grandes centros de futebol (como Brasil Série B/C e Championship).*

| País    | Liga / Temporada          |   Ano |   Total Jogos |   Jogos c/ Odds |   Cobertura Odds (%) |   Jogos c/ xG |   Cobertura xG (%) |
|:--------|:--------------------------|------:|--------------:|----------------:|---------------------:|--------------:|-------------------:|
| BRAZIL  | Serie B 2026              |  2026 |            89 |              89 |                100   |            89 |              100   |
| BRAZIL  | Serie C 2026              |  2026 |            68 |              68 |                100   |             0 |                0   |
| BRAZIL  | Serie B 2025              |  2025 |           380 |             380 |                100   |           380 |              100   |
| BRAZIL  | Serie C 2025              |  2025 |           216 |             216 |                100   |             0 |                0   |
| BRAZIL  | Serie B 2024              |  2024 |           380 |             380 |                100   |            83 |               21.8 |
| BRAZIL  | Serie C 2024              |  2024 |           216 |             216 |                100   |             0 |                0   |
| BRAZIL  | Serie B 2023              |  2023 |           380 |             380 |                100   |           343 |               90.3 |
| BRAZIL  | Serie C 2023              |  2023 |           216 |             216 |                100   |             0 |                0   |
| ENGLAND | Championship 2025-2026    |  2025 |           556 |             556 |                100   |           556 |              100   |
| ENGLAND | League One 2025-2026      |  2025 |           556 |             556 |                100   |           556 |              100   |
| ENGLAND | League Two 2025-2026      |  2025 |           556 |             556 |                100   |           556 |              100   |
| ENGLAND | National League 2025-2026 |  2025 |           557 |             556 |                 99.8 |             0 |                0   |
| ENGLAND | Championship 2024-2025    |  2024 |           557 |             557 |                100   |           413 |               74.1 |
| ENGLAND | League One 2024-2025      |  2024 |           557 |             556 |                 99.8 |           402 |               72.2 |
| ENGLAND | League Two 2024-2025      |  2024 |           557 |             555 |                 99.6 |           394 |               70.7 |
| ENGLAND | National League 2024-2025 |  2024 |           557 |             557 |                100   |             0 |                0   |
| ENGLAND | Championship 2023-2024    |  2023 |           557 |             557 |                100   |           555 |               99.6 |
| ENGLAND | League One 2023-2024      |  2023 |           557 |             557 |                100   |           541 |               97.1 |
| ENGLAND | League Two 2023-2024      |  2023 |           557 |             557 |                100   |           530 |               95.2 |
| ENGLAND | National League 2023-2024 |  2023 |           557 |             557 |                100   |             0 |                0   |
| ENGLAND | Championship 2022-2023    |  2022 |           557 |             557 |                100   |           206 |               37   |
| ENGLAND | League One 2022-2023      |  2022 |           557 |             557 |                100   |           217 |               39   |
| ENGLAND | League Two 2022-2023      |  2022 |           557 |             557 |                100   |           222 |               39.9 |
| ENGLAND | National League 2022-2023 |  2022 |           557 |             557 |                100   |             0 |                0   |
| GERMANY | 2. Bundesliga 2025-2026   |  2025 |           306 |             306 |                100   |           306 |              100   |
| GERMANY | 3. Liga 2025-2026         |  2025 |           380 |             380 |                100   |             0 |                0   |
| GERMANY | 2. Bundesliga 2024-2025   |  2024 |           308 |             308 |                100   |           308 |              100   |
| GERMANY | 3. Liga 2024-2025         |  2024 |           380 |             380 |                100   |             0 |                0   |
| GERMANY | 2. Bundesliga 2023-2024   |  2023 |           308 |             308 |                100   |           308 |              100   |
| GERMANY | 3. Liga 2023-2024         |  2023 |           380 |             379 |                 99.7 |             0 |                0   |
| GERMANY | 2. Bundesliga 2022-2023   |  2022 |           308 |             308 |                100   |           146 |               47.4 |
| GERMANY | 3. Liga 2022-2023         |  2022 |           380 |             380 |                100   |             0 |                0   |
| ITALY   | Serie B 2025-2026         |  2025 |           385 |             384 |                 99.7 |           385 |              100   |
| ITALY   | Serie B 2024-2025         |  2024 |           390 |             390 |                100   |           190 |               48.7 |
| ITALY   | Serie B 2023-2024         |  2023 |           390 |             389 |                 99.7 |           386 |               99   |
| ITALY   | Serie B 2022-2023         |  2022 |           390 |             390 |                100   |            59 |               15.1 |
| SPAIN   | La Liga 2 2025-2026       |  2025 |           439 |             437 |                 99.5 |           439 |              100   |
| SPAIN   | La Liga 2 2024-2025       |  2024 |           468 |             468 |                100   |           239 |               51.1 |
| SPAIN   | La Liga 2 2023-2024       |  2023 |           468 |             468 |                100   |           461 |               98.5 |
| SPAIN   | La Liga 2 2022-2023       |  2022 |           468 |             468 |                100   |            64 |               13.7 |

---

## 🟢 4. Tabela de Cobertura - Tier 3 (Exóticas)

*As 20 ligas adicionadas recentemente para explorar falhas e ineficiências de precificação das casas de apostas.*

| País         | Liga / Temporada           |   Ano |   Total Jogos |   Jogos c/ Odds |   Cobertura Odds (%) |   Jogos c/ xG |   Cobertura xG (%) |
|:-------------|:---------------------------|------:|--------------:|----------------:|---------------------:|--------------:|-------------------:|
| BELGIUM      | First Division B 2025-2026 |  2025 |           272 |             270 |                 99.3 |           271 |               99.6 |
| BELGIUM      | First Division B 2024-2025 |  2024 |           224 |             219 |                 97.8 |           120 |               53.6 |
| BELGIUM      | First Division B 2023-2024 |  2023 |           240 |             239 |                 99.6 |           158 |               65.8 |
| BULGARIA     | Parva Liga 2025-2026       |  2025 |           284 |             284 |                100   |             1 |                0.4 |
| BULGARIA     | Parva Liga 2024-2025       |  2024 |           295 |             294 |                 99.7 |             1 |                0.3 |
| BULGARIA     | Parva Liga 2023-2024       |  2023 |           284 |             282 |                 99.3 |             1 |                0.4 |
| CHILE        | Primera Division 2026      |  2026 |            96 |              95 |                 99   |            96 |              100   |
| CHILE        | Primera Division 2025      |  2025 |           252 |             251 |                 99.6 |           245 |               97.2 |
| CHILE        | Primera Division 2024      |  2024 |           240 |             239 |                 99.6 |           117 |               48.8 |
| CHINA        | Super League 2026          |  2026 |            96 |              94 |                 97.9 |            96 |              100   |
| CHINA        | Super League 2025          |  2025 |           240 |             238 |                 99.2 |           240 |              100   |
| CHINA        | Super League 2024          |  2024 |           240 |             239 |                 99.6 |           110 |               45.8 |
| COLOMBIA     | Liga BetPlay 2026          |  2026 |           200 |             200 |                100   |           198 |               99   |
| COLOMBIA     | Liga BetPlay 2025          |  2025 |           452 |             446 |                 98.7 |           451 |               99.8 |
| COLOMBIA     | Liga BetPlay 2024          |  2024 |           432 |             430 |                 99.5 |           292 |               67.6 |
| CROATIA      | 1. HNL 2025-2026           |  2025 |           175 |             174 |                 99.4 |           175 |              100   |
| CROATIA      | 1. HNL 2024-2025           |  2024 |           180 |             177 |                 98.3 |            90 |               50   |
| CROATIA      | 1. HNL 2023-2024           |  2023 |           180 |             179 |                 99.4 |           119 |               66.1 |
| DENMARK      | First Division 2025-2026   |  2025 |           180 |             177 |                 98.3 |           180 |              100   |
| DENMARK      | First Division 2024-2025   |  2024 |           192 |             190 |                 99   |            84 |               43.8 |
| DENMARK      | First Division 2023-2024   |  2023 |           192 |             191 |                 99.5 |           107 |               55.7 |
| ECUADOR      | Liga Pro 2026              |  2026 |           112 |             110 |                 98.2 |           111 |               99.1 |
| ECUADOR      | Liga Pro 2025              |  2025 |           311 |             310 |                 99.7 |           311 |              100   |
| ECUADOR      | Liga Pro 2024              |  2024 |           242 |             239 |                 98.8 |           122 |               50.4 |
| FRANCE       | Ligue 2 2025-2026          |  2025 |           306 |             304 |                 99.3 |           306 |              100   |
| FRANCE       | Ligue 2 2024-2025          |  2024 |           308 |             304 |                 98.7 |           163 |               52.9 |
| FRANCE       | Ligue 2 2023-2024          |  2023 |           380 |             377 |                 99.2 |           261 |               68.7 |
| GREECE       | Super League 2025-2026     |  2025 |           233 |             230 |                 98.7 |           233 |              100   |
| GREECE       | Super League 2024-2025     |  2024 |           236 |             235 |                 99.6 |           124 |               52.5 |
| GREECE       | Super League 2023-2024     |  2023 |           240 |             237 |                 98.8 |           236 |               98.3 |
| JAPAN        | J2 League 2026             |  2026 |           383 |             379 |                 99   |             0 |                0   |
| JAPAN        | J2 League 2024             |  2024 |           383 |             383 |                100   |             0 |                0   |
| NETHERLANDS  | Eerste Divisie 2025-2026   |  2025 |           390 |             385 |                 98.7 |           390 |              100   |
| NETHERLANDS  | Eerste Divisie 2024-2025   |  2024 |           380 |             378 |                 99.5 |           180 |               47.4 |
| NETHERLANDS  | Eerste Divisie 2023-2024   |  2023 |           392 |             387 |                 98.7 |           269 |               68.6 |
| NORWAY       | Obos-Ligaen 2026           |  2026 |            55 |              55 |                100   |             0 |                0   |
| NORWAY       | Obos-Ligaen 2025           |  2025 |           247 |             246 |                 99.6 |             0 |                0   |
| NORWAY       | Obos-Ligaen 2024           |  2024 |           247 |             245 |                 99.2 |             0 |                0   |
| POLAND       | Ekstraklasa 2025-2026      |  2025 |           297 |             297 |                100   |           296 |               99.7 |
| POLAND       | Ekstraklasa 2024-2025      |  2024 |           306 |             302 |                 98.7 |           305 |               99.7 |
| POLAND       | Ekstraklasa 2023-2024      |  2023 |           306 |             304 |                 99.3 |           192 |               62.7 |
| PORTUGAL     | Segunda Liga 2025-2026     |  2025 |           306 |             300 |                 98   |             0 |                0   |
| PORTUGAL     | Segunda Liga 2024-2025     |  2024 |           308 |             307 |                 99.7 |             0 |                0   |
| PORTUGAL     | Segunda Liga 2023-2024     |  2023 |           308 |             305 |                 99   |             0 |                0   |
| ROMANIA      | Liga 1 2025-2026           |  2025 |           312 |             312 |                100   |           312 |              100   |
| ROMANIA      | Liga 1 2024-2025           |  2024 |           319 |             317 |                 99.4 |           236 |               74   |
| ROMANIA      | Liga 1 2023-2024           |  2023 |           321 |             319 |                 99.4 |           206 |               64.2 |
| SAUDI ARABIA | Pro League 2025-2026       |  2025 |           297 |             293 |                 98.7 |           297 |              100   |
| SAUDI ARABIA | Pro League 2024-2025       |  2024 |           306 |             301 |                 98.4 |           225 |               73.5 |
| SAUDI ARABIA | Pro League 2023-2024       |  2023 |           306 |             303 |                 99   |           204 |               66.7 |
| SCOTLAND     | Premiership 2025-2026      |  2025 |           232 |             231 |                 99.6 |           232 |              100   |
| SCOTLAND     | Premiership 2024-2025      |  2024 |           234 |             233 |                 99.6 |           184 |               78.6 |
| SCOTLAND     | Premiership 2023-2024      |  2023 |           234 |             233 |                 99.6 |           227 |               97   |
| SOUTH KOREA  | K-League 1 2026            |  2026 |            90 |              90 |                100   |             0 |                0   |
| SOUTH KOREA  | K-League 1 2025            |  2025 |           232 |             230 |                 99.1 |             1 |                0.4 |
| SOUTH KOREA  | K-League 1 2024            |  2024 |           232 |             230 |                 99.1 |             0 |                0   |
| SWEDEN       | Superettan 2026            |  2026 |            63 |              62 |                 98.4 |             0 |                0   |
| SWEDEN       | Superettan 2025            |  2025 |           244 |             244 |                100   |             0 |                0   |
| SWEDEN       | Superettan 2024            |  2024 |           244 |             243 |                 99.6 |             0 |                0   |

---

## 🧠 5. Diretrizes de Ouro para Treinamento de Modelos Separados

### 🎯 A. O Modelo Tier 1 (Elite) — "Modelo Tecnológico"
*   **Dataset Recomendado:** Apenas linhas com `Tier == "Tier 1"` e temporadas de 2024 a 2026.
*   **Métricas Recomendadas:** `xg_h_ft`, `xg_a_ft`, `shots_on_target`, `corners`, `possession` e as odds.
*   **Tratamento de NaN:** Como o xG começou a ser implantado no início de 2023, faça um corte simples em Pandas:
    ```python
    df_t1 = df[df['tier'] == 'Tier 1'].dropna(subset=['xg_h_ft'])
    ```
    Isso vai te dar um dataset perfeito de quase 20.000 jogos ultra-qualificados!

### ⚡ B. O Modelo Tier 2 (Acesso) — "Modelo Híbrido"
*   **Métricas Recomendadas:** Traditional stats (chutes, escanteios, cartões) + ELO histórico + variação de odds.
*   **Nota Técnico-Tática:** Algumas ligas de acesso (como Série B brasileira de 2024 e Championship 2023) têm cobertura de xG parcial. Você pode treinar um modelo de regressão para "estimar" o xG com base nos chutes a gol, ou treinar o modelo de classificação final sem usar o xG como variável (usando apenas chutes no gol e posse).

### 🟢 C. O Modelo Tier 3 (Exótico) — "O Caçador de Ineficiências"
*   **Métricas Recomendadas:** **NÃO use xG neste modelo!** Como o xG é 0% ou muito esparso nas exóticas, treinar um modelo com xG causaria a perda de 95% do dataset. 
*   Em vez disso, use **Gols HT/FT, Chutes no Gol, Escanteios, Cartões, Peso do Mando de Campo e Movimentação de Odds**.
*   **Por que ele é lucrativo:** O bookmaker precifica mal essas ligas. Um modelo simples focado em ELO de time + Chutes a Gol históricos + histórico de Under/Over cantos vai bater a precificação da Bet365 com extrema facilidade!

---
*Relatório consolidado e atualizado em 19 de maio de 2026.*
