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
    "Brasil - Serie A": {
        "url_base": "https://www.flashscore.com.br/futebol/brasil/brasileirao-betano",
        "pais": "BRAZIL",
        "liga": "Série A",
        "div": "Serie A Betano",
        "league_code": "BRAZIL 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Inglaterra - Premier League": {
        "url_base": "https://www.flashscore.com.br/futebol/inglaterra/premier-league",
        "pais": "ENGLAND",
        "liga": "Premier League",
        "div": "Premier League",
        "league_code": "ENGLAND 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Espanha - La Liga": {
        "url_base": "https://www.flashscore.com.br/futebol/espanha/laliga",
        "pais": "SPAIN",
        "liga": "La Liga",
        "div": "La Liga",
        "league_code": "SPAIN 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Itália - Serie A": {
        "url_base": "https://www.flashscore.com.br/futebol/italia/serie-a",
        "pais": "ITALY",
        "liga": "Serie A",
        "div": "Serie A",
        "league_code": "ITALY 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Alemanha - Bundesliga": {
        "url_base": "https://www.flashscore.com.br/futebol/alemanha/bundesliga",
        "pais": "GERMANY",
        "liga": "Bundesliga",
        "div": "Bundesliga",
        "league_code": "GERMANY 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "França - Ligue 1": {
        "url_base": "https://www.flashscore.com.br/futebol/franca/ligue-1",
        "pais": "FRANCE",
        "liga": "Ligue 1",
        "div": "Ligue 1",
        "league_code": "FRANCE 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Países Baixos - Eredivisie": {
        "url_base": "https://www.flashscore.com.br/futebol/paises-baixos/eredivisie",
        "pais": "NETHERLANDS",
        "liga": "Eredivisie",
        "div": "Eredivisie",
        "league_code": "NETHERLANDS 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Portugal - Primeira Liga": {
        "url_base": "https://www.flashscore.com.br/futebol/portugal/liga-portugal",
        "pais": "PORTUGAL",
        "liga": "Primeira Liga",
        "div": "Primeira Liga",
        "league_code": "PORTUGAL 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Bélgica - Pro League": {
        "url_base": "https://www.flashscore.com.br/futebol/belgica/liga-jupiler",
        "pais": "BELGIUM",
        "liga": "Pro League",
        "div": "Pro League",
        "league_code": "BELGIUM 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "EUA - Major League Soccer": {
        "url_base": "https://www.flashscore.com.br/futebol/estados-unidos/mls",
        "pais": "USA",
        "liga": "MLS",
        "div": "MLS",
        "league_code": "USA 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "México - Liga MX": {
        "url_base": "https://www.flashscore.com.br/futebol/mexico/liga-mx",
        "pais": "MEXICO",
        "liga": "Liga MX",
        "div": "Liga MX",
        "league_code": "MEXICO 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Japão - J-League": {
        "url_base": "https://www.flashscore.com.br/futebol/japao/liga-j1",
        "pais": "JAPAN",
        "liga": "J1 League",
        "div": "J1 League",
        "league_code": "JAPAN 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Argentina - Superliga": {
        "url_base": "https://www.flashscore.com.br/futebol/argentina/liga-profissional",
        "pais": "ARGENTINA",
        "liga": "Liga Profesional",
        "div": "Liga Profesional",
        "league_code": "ARGENTINA 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Austrália - A-League": {
        "url_base": "https://www.flashscore.com.br/futebol/australia/a-league",
        "pais": "AUSTRALIA",
        "liga": "A-League",
        "div": "A-League",
        "league_code": "AUSTRALIA 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Turquia - Super Lig": {
        "url_base": "https://www.flashscore.com.br/futebol/turquia/super-lig",
        "pais": "TURKEY",
        "liga": "Super Lig",
        "div": "Super Lig",
        "league_code": "TURKEY 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Suécia - Allsvenskan": {
        "url_base": "https://www.flashscore.com.br/futebol/suecia/allsvenskan",
        "pais": "SWEDEN",
        "liga": "Allsvenskan",
        "div": "Allsvenskan",
        "league_code": "SWEDEN 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Noruega - Eliteserien": {
        "url_base": "https://www.flashscore.com.br/futebol/noruega/eliteserien",
        "pais": "NORWAY",
        "liga": "Eliteserien",
        "div": "Eliteserien",
        "league_code": "NORWAY 1",
        "temporadas": ["2026", "2025", "2024"]
    },
    
    "Suíça - Super League": {
        "url_base": "https://www.flashscore.com.br/futebol/suica/super-league",
        "pais": "SWITZERLAND",
        "liga": "Super League",
        "div": "Super League",
        "league_code": "SWITZERLAND 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Áustria - Tipico Bundesliga": {
        "url_base": "https://www.flashscore.com.br/futebol/austria/bundesliga",
        "pais": "AUSTRIA",
        "liga": "Bundesliga",
        "div": "Bundesliga",
        "league_code": "AUSTRIA 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },
    
    "Dinamarca - Superliga": {
        "url_base": "https://www.flashscore.com.br/futebol/dinamarca/superliga",
        "pais": "DENMARK",
        "liga": "Superliga",
        "div": "Superliga",
        "league_code": "DENMARK 1",
        "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    },

    # =========================================================================
    # 🟡 TIER 2 E DEMAIS (INATIVAS / COMENTADAS - FÁCIL ATIVAÇÃO SE NECESSÁRIO)
    # =========================================================================
    
    # "Brasil - Serie B": {
    #     "url_base": "https://www.flashscore.com.br/futebol/brasil/serie-b",
    #     "pais": "BRAZIL",
    #     "liga": "Série B",
    #     "div": "Serie B",
    #     "league_code": "BRAZIL 2",
    #     "temporadas": ["2026", "2025", "2024"]
    # },
    
    # "Brasil - Serie C": {
    #     "url_base": "https://www.flashscore.com.br/futebol/brasil/serie-c",
    #     "pais": "BRAZIL",
    #     "liga": "Série C",
    #     "div": "Serie C",
    #     "league_code": "BRAZIL 3",
    #     "temporadas": ["2026", "2025", "2024"]
    # },
    
    # "Inglaterra - Championship": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/championship",
    #     "pais": "ENGLAND",
    #     "liga": "Championship",
    #     "div": "Championship",
    #     "league_code": "ENGLAND 2",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Inglaterra - League One": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/league-one",
    #     "pais": "ENGLAND",
    #     "liga": "League One",
    #     "div": "League One",
    #     "league_code": "ENGLAND 3",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Inglaterra - League Two": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/league-two",
    #     "pais": "ENGLAND",
    #     "liga": "League Two",
    #     "div": "League Two",
    #     "league_code": "ENGLAND 4",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Inglaterra - National League": {
    #     "url_base": "https://www.flashscore.com.br/futebol/inglaterra/national-league",
    #     "pais": "ENGLAND",
    #     "liga": "National League",
    #     "div": "National League",
    #     "league_code": "ENGLAND 5",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Espanha - La Liga 2": {
    #     "url_base": "https://www.flashscore.com.br/futebol/espanha/laliga2",
    #     "pais": "SPAIN",
    #     "liga": "La Liga 2",
    #     "div": "La Liga 2",
    #     "league_code": "SPAIN 2",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Itália - Serie B": {
    #     "url_base": "https://www.flashscore.com.br/futebol/italia/serie-b",
    #     "pais": "ITALY",
    #     "liga": "Serie B",
    #     "div": "Serie B",
    #     "league_code": "ITALY 2",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Alemanha - 2. Bundesliga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/alemanha/2-bundesliga",
    #     "pais": "GERMANY",
    #     "liga": "2. Bundesliga",
    #     "div": "2. Bundesliga",
    #     "league_code": "GERMANY 2",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # },
    
    # "Alemanha - 3. Liga": {
    #     "url_base": "https://www.flashscore.com.br/futebol/alemanha/3-liga",
    #     "pais": "GERMANY",
    #     "liga": "3. Liga",
    #     "div": "3. Liga",
    #     "league_code": "GERMANY 3",
    #     "temporadas": ["2025-2026", "2024-2025", "2023-2024"]
    # }
}
