# Skill: Pipeline Profissional de Coleta em 3 Etapas (Flashscore + ML + StatsGreen)

# Objetivo

Esta skill define a arquitetura oficial do pipeline de coleta de dados.

O sistema será dividido em:

1. MATCHES
2. ODDS
3. STATS

Todos conectados por:

```text
match_id
```

A arquitetura foi criada para:

- escalabilidade
- integridade
- performance
- reprocessamento
- ML
- frontend profissional
- múltiplas temporadas
- múltiplas ligas

---

# Filosofia da arquitetura

## NÃO fazer scraping monolítico.

Evitar:

```text
abrir jogo
↓
coletar tudo
↓
salvar csv gigante
```

Modelo correto:

```text
MATCHES
↓
ODDS
↓
STATS
```

---

# Estrutura geral do sistema

```text
Flashscore
↓
Collector
↓
RAW JSON
↓
Parser
↓
SQLite/PostgreSQL
↓
Feature Engineering
↓
ML
↓
Django API
↓
Frontend
```

---

# ETAPA 1 — MATCHES

# Objetivo

Criar a base histórica principal do projeto.

Essa é a tabela mais importante.

Ela deve conter:

- jogos
- IDs
- resultados
- datas
- ligas
- temporadas

Sem estatísticas avançadas.

---

# Características da etapa

## Mais rápida
## Mais estável
## Menor risco
## Alta escalabilidade

---

# Tabela: matches

## Colunas obrigatórias

```text
match_id
league_id
league_name
country
season
round

match_date
match_time
timestamp
status

home_team_id
home_team

away_team_id
away_team

home_score_ft
away_score_ft

home_score_ht
away_score_ht

winner
is_draw

home_position
away_position

venue

source
created_at
updated_at
```

---

# Objetivo técnico da etapa

Construir:

## histórico confiável

para:

- ML
- odds
- stats
- enriquecimento futuro

---

# O que NÃO coletar nesta etapa

## NÃO incluir:

- xG
- shots
- corners
- cards
- odds detalhadas
- eventos
- lineups

---

# Estratégia recomendada

## Coletar:

- 78 ligas
- 3 temporadas inicialmente

---

# Benefícios

## Base histórica rápida.

Mesmo sem stats avançadas.

---

# ETAPA 2 — ODDS

# Objetivo

Enriquecer as partidas com odds históricas.

Odds são uma das features mais importantes do futebol.

---

# Características da etapa

## Média complexidade
## Atualizações frequentes
## Alta importância para ML

---

# Tabela: odds

## Odds FT

```text
match_id

home_odds_open
draw_odds_open
away_odds_open

home_odds_close
draw_odds_close
away_odds_close
```

---

# Odds Over/Under

```text
over_25_open
under_25_open

over_25_close
under_25_close
```

---

# BTTS

```text
btts_yes_open
btts_no_open

btts_yes_close
btts_no_close
```

---

# Metadata

```text
bookmaker
odds_timestamp
source
created_at
updated_at
```

---

# Objetivo técnico da etapa

Adicionar:

- valor de mercado implícito
- expectativa de gols
- favoritismo
- probabilidade implícita

---

# Benefícios para ML

## Features extremamente fortes:

- closing odds
- opening odds
- over/under
- BTTS

---

# Estratégia recomendada

## NÃO recolher jogos.

Usar:

```text
match_id
```

existente na tabela:

```text
matches
```

---

# Fluxo correto

```text
matches
↓
coleta odds
↓
odds table
```

---

# ETAPA 3 — STATS

# Objetivo

Enriquecer partidas com estatísticas avançadas.

Essa é a etapa:

- mais pesada
- mais lenta
- mais sujeita a falhas

---

# Características da etapa

## Alta complexidade
## Alto volume
## Maior risco operacional

---

# Tabela: stats

# xG

```text
match_id

home_xg
away_xg

home_xgot
away_xgot
```

---

# Shots

```text
home_shots
away_shots

home_shots_on_target
away_shots_on_target
```

---

# Corners

```text
home_corners
away_corners
```

---

# Dangerous attacks

```text
home_dangerous_attacks
away_dangerous_attacks
```

---

# Possession

```text
home_possession
away_possession
```

---

# Cards

```text
home_yellow_cards
away_yellow_cards

home_red_cards
away_red_cards
```

---

# HT stats (opcional)

```text
home_xg_ht
away_xg_ht
```

---

# Metadata

```text
stats_available
source
created_at
updated_at
```

---

# O que priorizar na etapa

## Mais importantes

- xG
- shots on target
- corners

---

# Menos importantes

- posse
- faltas
- ataques perigosos

---

# Objetivo técnico da etapa

Criar:

## features avançadas para ML

---

# Estratégia correta

## NÃO recolher fixtures.

Usar:

```text
match_id
```

para enriquecer partidas já existentes.

---

# Estrutura final do banco

```text
matches
    1 → 1 odds
    1 → 1 stats
```

---

# Fluxo completo ideal

```text
1. coletar fixtures
2. salvar matches
3. coletar odds
4. salvar odds
5. coletar stats
6. salvar stats
7. validar integridade
8. feature engineering
9. exportar datasets
10. ML
```

---

# Vantagens da arquitetura

# 1. Reprocessamento fácil

Se parser quebrar:

```text
reprocessar apenas stats
```

---

# 2. Mais velocidade

Não recolher tudo repetidamente.

---

# 3. Melhor integridade

Tudo baseado em:

```text
match_id
```

---

# 4. Mais escalabilidade

Suporta:

- 100k+
- 200k+
- múltiplas temporadas
- múltiplas ligas

---

# 5. Melhor manutenção

Separação clara:

- fixtures
- odds
- stats

---

# Estrutura recomendada do projeto

```text
project/
│
├── collector/
│   ├── matches/
│   ├── odds/
│   └── stats/
│
├── raw/
│
├── parser/
│   ├── matches_parser.py
│   ├── odds_parser.py
│   └── stats_parser.py
│
├── validators/
│
├── database/
│
├── feature_engineering/
│
├── ml/
│
└── api/
```

---

# Estratégia operacional ideal

# FASE 1

## 78 ligas
## 3 temporadas
## Apenas matches

---

# FASE 2

Adicionar:

- odds
- over/under
- BTTS

---

# FASE 3

Adicionar:

- xG
- shots
- corners

---

# FASE 4

Feature engineering:

- rolling xG
- rolling odds
- elo
- momentum
- home/away strength

---

# FASE 5

ML:

- treinamento
- validação
- inferência

---

# Skill principal da arquitetura

## Separação de responsabilidades.

Cada etapa deve ter:

- coleta própria
- parser próprio
- validação própria
- tabela própria

---

# Skill crítica

## Integridade histórica.

Mais importante do que:

- quantidade absurda de stats
- quantidade de features
- scraping complexo

---

# Objetivo final

Transformar o projeto em:

## plataforma profissional de inteligência esportiva

com:

- histórico confiável
- pipeline escalável
- ML robusto
- frontend profissional
- arquitetura sustentável

