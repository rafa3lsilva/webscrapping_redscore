# dados consolidados
LIGAS_PERMITIDAS = {
    "África do Sul - Premier League",
    "Albânia - Superliga",
    "Alemanha - 2. Bundesliga",
    "Alemanha - 3. Liga",
    "Alemanha - Bundesliga",
    "Argélia - Ligue 1",
    "Argentina - Primera B Nacional",
    "Argentina - Superliga",
    "Arábia Saudita - Pro League",
    "Armênia - Premier League",
    "Austrália - A-League",
    "Áustria - Tipico Bundesliga",
    "Bélgica - First Division B",
    "Bélgica - Pro League",
    "Bolívia - Liga De Futbol Prof",
    "Brasil - Serie A",
    "Brasil - Serie B",
    "Brasil - Serie C",
    "Bulgária - Parva Liga",
    "Chile - Primera Division",
    "China - Super League",
    "Colômbia - Liga BetPlay",
    "Coréia do Sul - K-League 1",
    "Coréia do Sul - K League 2",
    "Croácia - 1. HNL",
    "Dinamarca - First Division",
    "Dinamarca - Superliga",
    "Egito - Premier League",
    "Equador - Liga Pro",
    "Escócia - Championship",
    "Escócia - League One",
    "Escócia - League Two",
    "Escócia - Premiership",
    "Eslováquia - Fortuna Liga",
    "Eslovênia - 1. SNL",
    "Espanha - La Liga",
    "Espanha - La Liga 2",
    "Estônia - Meistriliiga",
    "EUA - Major League Soccer",
    "Finlândia - Veikkausliiga",
    "França - Ligue 1",
    "França - Ligue 2",
    "Grécia - Super League",
    "Hungria - OTP Bank Liga",
    "Inglaterra - Championship",
    "Inglaterra - League One",
    "Inglaterra - League Two",
    "Inglaterra - National League",
    "Inglaterra - Premier League",
    "Irlanda - Premier Division",
    "Irlanda do Norte - Premiership",
    "Islândia - Pepsideild",
    "Israel - Ligat ha'Al",
    "Itália - Serie A",
    "Itália - Serie B",
    "Japão - J-League",
    "Japão - J2-League",
    "Lituânia - A Lyga",
    "Malásia - Super League",
    "México - Liga MX",
    "Noruega - Eliteserien",
    "Noruega - Obos-Ligaen",
    "Países Baixos - Eerste Divisie",
    "Países Baixos - Eredivisie",
    "País de Gales - Premier League",
    "Paraguai - Division 1",
    "Peru - Primera Division",
    "Polônia - Ekstraklasa",
    "Portugal - Primeira Liga",
    "Portugal - Segunda Liga",
    "Romênia - Liga 1",
    "Sérvia - Super Liga",
    "Suécia - Allsvenskan",
    "Suécia - Superettan",
    "Suíça - Super League",
    "Turquia - Super Lig",
    "Uruguai - Primera Division",
    "Venezuela - Primera Division",
}

LIMITE_JOGOS_POR_TIME = 50 # faz parte dos dados consolidados


# =========================================================================
# 🟢 TIER 1: ELITE (xG ~100% + Odds Completas - FOCO EM ATUAL + 2)
# =========================================================================
LIGAS_FLASHSCORE = {
    # "Brasil - Serie A": {
    #     "url_base": "https://www.flashscore.com.br/futebol/brasil/brasileirao-betano",
    #     "pais": "BRAZIL",
    #     "liga": "Série A",
    #     "div": "Serie A Betano",
    #     "league_code": "BRAZIL 1",
    #     "temporadas": ["2026"]
    # },
    
    # "Inglaterra - Premier League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/premier-league",
    #     "pais": "ENGLAND",
    #     "liga": "Premier League",
    #     "div": "Premier League",
    #     "league_code": "ENGLAND 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Espanha - La Liga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/espanha/laliga",
    #     "pais": "SPAIN",
    #     "liga": "La Liga",
    #     "div": "La Liga",
    #     "league_code": "SPAIN 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Itália - Serie A": {
    #     "url_base": "https://www.flashscore.com.br/futebol/italia/serie-a",
    #     "pais": "ITALY",
    #     "liga": "Serie A",
    #     "div": "Serie A",
    #     "league_code": "ITALY 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Alemanha - Bundesliga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/alemanha/bundesliga",
    #     "pais": "GERMANY",
    #     "liga": "Bundesliga",
    #     "div": "Bundesliga",
    #     "league_code": "GERMANY 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "França - Ligue 1": {
    #     "url_base": "https://www.flashscore.com.br/futebol/franca/ligue-1",
    #     "pais": "FRANCE",
    #     "liga": "Ligue 1",
    #     "div": "Ligue 1",
    #     "league_code": "FRANCE 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Países Baixos - Eredivisie": {
    #     "url_base": "https://www.flashscore.com.br/futebol/holanda/eredivisie",
    #     "pais": "NETHERLANDS",
    #     "liga": "Eredivisie",
    #     "div": "Eredivisie",
    #     "league_code": "NETHERLANDS 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Portugal - Primeira Liga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/portugal/liga-portugal",
    #     "pais": "PORTUGAL",
    #     "liga": "Primeira Liga",
    #     "div": "Primeira Liga",
    #     "league_code": "PORTUGAL 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Bélgica - Pro League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/belgica/liga-jupiler",
    #     "pais": "BELGIUM",
    #     "liga": "Pro League",
    #     "div": "Pro League",
    #     "league_code": "BELGIUM 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "EUA - Major League Soccer": {
    #     "url_base": "https://www.flashscore.com.br/futebol/eua/mls",
    #     "pais": "USA",
    #     "liga": "MLS",
    #     "div": "MLS",
    #     "league_code": "USA 1",
    #     "temporadas": ["2026"]
    # },
    
    # "México - Liga MX": {
    #     "url_base": "https://www.flashscore.com.br/futebol/mexico/liga-mx",
    #     "pais": "MEXICO",
    #     "liga": "Liga MX",
    #     "div": "Liga MX",
    #     "league_code": "MEXICO 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Japão - J-League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/japao/liga-j1",
    #     "pais": "JAPAN",
    #     "liga": "J1 League",
    #     "div": "J1 League",
    #     "league_code": "JAPAN 1",
    #     "temporadas": ["2026"]
    # },
    
    # "Argentina - Superliga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/argentina/liga-profesional",
    #     "pais": "ARGENTINA",
    #     "liga": "Liga Profesional",
    #     "div": "Liga Profesional",
    #     "league_code": "ARGENTINA 1",
    #     "temporadas": ["2026"]
    # },
    
    # "Austrália - A-League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/australia/a-league",
    #     "pais": "AUSTRALIA",
    #     "liga": "A-League",
    #     "div": "A-League",
    #     "league_code": "AUSTRALIA 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Turquia - Super Lig": {
    #     "url_base": "https://www.flashscore.com.br/futebol/turquia/super-lig",
    #     "pais": "TURKEY",
    #     "liga": "Super Lig",
    #     "div": "Super Lig",
    #     "league_code": "TURKEY 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Suécia - Allsvenskan": {
    #     "url_base": "https://www.flashscore.com.br/futebol/suecia/allsvenskan",
    #     "pais": "SWEDEN",
    #     "liga": "Allsvenskan",
    #     "div": "Allsvenskan",
    #     "league_code": "SWEDEN 1",
    #     "temporadas": ["2026"]
    # },
    
    # "Noruega - Eliteserien": {
    #     "url_base": "https://www.flashscore.com.br/futebol/noruega/eliteserien",
    #     "pais": "NORWAY",
    #     "liga": "Eliteserien",
    #     "div": "Eliteserien",
    #     "league_code": "NORWAY 1",
    #     "temporadas": ["2026"]
    # },
    
    # "Suíça - Super League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/suica/super-league",
    #     "pais": "SWITZERLAND",
    #     "liga": "Super League",
    #     "div": "Super League",
    #     "league_code": "SWITZERLAND 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Áustria - Tipico Bundesliga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/austria/bundesliga",
    #     "pais": "AUSTRIA",
    #     "liga": "Bundesliga",
    #     "div": "Bundesliga",
    #     "league_code": "AUSTRIA 1",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Dinamarca - Superliga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/dinamarca/superliga",
    #     "pais": "DENMARK",
    #     "liga": "Superliga",
    #     "div": "Superliga",
    #     "league_code": "DENMARK 1",
    #     "temporadas": ["2025-2026"]
    # },

    # # =========================================================================
    # # 🟡 TIER 2 E DEMAIS (INATIVAS / COMENTADAS - FÁCIL ATIVAÇÃO SE NECESSÁRIO)
    # # =========================================================================
    
    # "Brasil - Serie B": {
    #     "url_base": "https://www.flashscore.com.br/futebol/brasil/serie-b",
    #     "pais": "BRAZIL",
    #     "liga": "Série B",
    #     "div": "Serie B",
    #     "league_code": "BRAZIL 2",
    #     "temporadas": ["2026"]
    # },
    
    # "Brasil - Serie C": {
    #     "url_base": "https://www.flashscore.com.br/futebol/brasil/serie-c",
    #     "pais": "BRAZIL",
    #     "liga": "Série C",
    #     "div": "Serie C",
    #     "league_code": "BRAZIL 3",
    #     "temporadas": ["2026"]
    # },
    
    # "Inglaterra - Championship": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/championship",
    #     "pais": "ENGLAND",
    #     "liga": "Championship",
    #     "div": "Championship",
    #     "league_code": "ENGLAND 2",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Inglaterra - League One": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/league-one",
    #     "pais": "ENGLAND",
    #     "liga": "League One",
    #     "div": "League One",
    #     "league_code": "ENGLAND 3",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Inglaterra - League Two": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/league-two",
    #     "pais": "ENGLAND",
    #     "liga": "League Two",
    #     "div": "League Two",
    #     "league_code": "ENGLAND 4",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Inglaterra - National League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/national-league",
    #     "pais": "ENGLAND",
    #     "liga": "National League",
    #     "div": "National League",
    #     "league_code": "ENGLAND 5",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Espanha - La Liga 2": {
    #     "url_base": "https://www.flashscore.com.br/futebol/espanha/laliga2",
    #     "pais": "SPAIN",
    #     "liga": "La Liga 2",
    #     "div": "La Liga 2",
    #     "league_code": "SPAIN 2",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Itália - Serie B": {
    #     "url_base": "https://www.flashscore.com.br/futebol/italia/serie-b",
    #     "pais": "ITALY",
    #     "liga": "Serie B",
    #     "div": "Serie B",
    #     "league_code": "ITALY 2",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Alemanha - 2. Bundesliga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/alemanha/2-bundesliga",
    #     "pais": "GERMANY",
    #     "liga": "2. Bundesliga",
    #     "div": "2. Bundesliga",
    #     "league_code": "GERMANY 2",
    #     "temporadas": ["2025-2026"]
    # },
    
    # "Alemanha - 3. Liga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/alemanha/3-liga",
    #     "pais": "GERMANY",
    #     "liga": "3. Liga",
    #     "div": "3. Liga",
    #     "league_code": "GERMANY 3",
    #     "temporadas": ["2025-2026"]
    # },

    
    # =========================================================================
    # 🟢 TIER 3: EXÓTICAS / ADICIONAIS (COBERTURA ESTATÍSTICA BÁSICA ESTÁVEL)
    # =========================================================================
    
    "França - Ligue 2": {
        "url_base": "https://www.flashscore.com.br/futebol/franca/ligue-2",
        "pais": "FRANCE",
        "liga": "Ligue 2",
        "div": "Ligue 2",
        "league_code": "FRANCE 2",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Escócia - Premiership": {
        "url_base": "https://www.flashscore.com.br/futebol/escocia/premiership",
        "pais": "SCOTLAND",
        "liga": "Premiership",
        "div": "Premiership",
        "league_code": "SCOTLAND 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Grécia - Super League": {
        "url_base": "https://www.flashscore.com.br/futebol/grecia/superliga",
        "pais": "GREECE",
        "liga": "Super League",
        "div": "Super League",
        "league_code": "GREECE 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Colômbia - Liga BetPlay": {
        "url_base": "https://www.flashscore.com.br/futebol/colombia/primera-a",
        "pais": "COLOMBIA",
        "liga": "Liga BetPlay",
        "div": "Liga BetPlay",
        "league_code": "COLOMBIA 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Chile - Primera Division": {
        "url_base": "https://www.flashscore.com.br/futebol/chile/liga-de-primera",
        "pais": "CHILE",
        "liga": "Primera Division",
        "div": "Primera Division",
        "league_code": "CHILE 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Equador - Liga Pro": {
        "url_base": "https://www.flashscore.com.br/futebol/equador/liga-pro",
        "pais": "ECUADOR",
        "liga": "Liga Pro",
        "div": "Liga Pro",
        "league_code": "ECUADOR 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Arábia Saudita - Pro League": {
        "url_base": "https://www.flashscore.com.br/futebol/arabia-saudita/primeira-liga",
        "pais": "SAUDI ARABIA",
        "liga": "Pro League",
        "div": "Pro League",
        "league_code": "SAUDI ARABIA 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Coréia do Sul - K-League 1": {
        "url_base": "https://www.flashscore.com.br/futebol/coreia-do-sul/liga-k-1",
        "pais": "SOUTH KOREA",
        "liga": "K-League 1",
        "div": "K-League 1",
        "league_code": "SOUTH KOREA 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "China - Super League": {
        "url_base": "https://www.flashscore.com.br/futebol/china/superliga",
        "pais": "CHINA",
        "liga": "Super League",
        "div": "Super League",
        "league_code": "CHINA 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Polônia - Ekstraklasa": {
        "url_base": "https://www.flashscore.com.br/futebol/polonia/ekstraklasa",
        "pais": "POLAND",
        "liga": "Ekstraklasa",
        "div": "Ekstraklasa",
        "league_code": "POLAND 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Croácia - 1. HNL": {
        "url_base": "https://www.flashscore.com.br/futebol/croacia/hnl",
        "pais": "CROATIA",
        "liga": "1. HNL",
        "div": "1. HNL",
        "league_code": "CROATIA 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Romênia - Liga 1": {
        "url_base": "https://www.flashscore.com.br/futebol/romenia/superliga",
        "pais": "ROMANIA",
        "liga": "Liga 1",
        "div": "Liga 1",
        "league_code": "ROMANIA 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Bulgária - Parva Liga": {
        "url_base": "https://www.flashscore.com.br/futebol/bulgaria/parva-liga",
        "pais": "BULGARIA",
        "liga": "Parva Liga",
        "div": "Parva Liga",
        "league_code": "BULGARIA 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Japão - J2-League": {
        "url_base": "https://www.flashscore.com.br/futebol/japao/liga-j2",
        "pais": "JAPAN",
        "liga": "J2-League",
        "div": "J2 League",
        "league_code": "JAPAN 2",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Suécia - Superettan": {
        "url_base": "https://www.flashscore.com.br/futebol/suecia/superettan",
        "pais": "SWEDEN",
        "liga": "Superettan",
        "div": "Superettan",
        "league_code": "SWEDEN 2",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Noruega - Obos-Ligaen": {
        "url_base": "https://www.flashscore.com.br/futebol/noruega/obos-ligaen",
        "pais": "NORWAY",
        "liga": "Obos-Ligaen",
        "div": "Obos-Ligaen",
        "league_code": "NORWAY 2",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Países Baixos - Eerste Divisie": {
        "url_base": "https://www.flashscore.com.br/futebol/holanda/eerste-divisie",
        "pais": "NETHERLANDS",
        "liga": "Eerste Divisie",
        "div": "Eerste Divisie",
        "league_code": "NETHERLANDS 2",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Portugal - Segunda Liga": {
        "url_base": "https://www.flashscore.com.br/futebol/portugal/liga-portugal-2",
        "pais": "PORTUGAL",
        "liga": "Segunda Liga",
        "div": "Segunda Liga",
        "league_code": "PORTUGAL 2",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Bélgica - First Division B": {
        "url_base": "https://www.flashscore.com.br/futebol/belgica/challenger-pro-league",
        "pais": "BELGIUM",
        "liga": "First Division B",
        "div": "First Division B",
        "league_code": "BELGIUM 2",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Dinamarca - First Division": {
        "url_base": "https://www.flashscore.com.br/futebol/dinamarca/1-divisao",
        "pais": "DENMARK",
        "liga": "First Division",
        "div": "First Division",
        "league_code": "DENMARK 2",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    }
}
