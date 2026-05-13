"""
Validadores de integridade de dados para partidas de futebol.

Alinhado com a Skill §14 e §15: O sistema deve validar times duplicados,
odds inválidas, jogos incompletos e dados inconsistentes.
"""
import logging

log = logging.getLogger("validators.match")


class MatchValidationError(Exception):
    """Exceção para dados de partida inválidos."""
    pass


def validar_partida(jogo: dict, strict: bool = False) -> list[str]:
    """
    Valida um dicionário de partida e retorna lista de problemas encontrados.
    
    Args:
        jogo: Dicionário com dados da partida
        strict: Se True, levanta exceção no primeiro erro
        
    Returns:
        Lista de strings descrevendo problemas (vazia se tudo OK)
    """
    problemas = []
    
    # 1. Times não podem ser iguais
    home = jogo.get("Home", "").strip()
    away = jogo.get("Away", "").strip()
    if home and away and home.lower() == away.lower():
        problemas.append(f"Home == Away: '{home}'")
    
    # 2. Times não podem estar vazios
    if not home:
        problemas.append("Home está vazio")
    if not away:
        problemas.append("Away está vazio")
    
    # 3. Odds devem ser > 1.0 (se presentes)
    for odd_key in ["Odd_H", "Odd_D", "Odd_A"]:
        val = jogo.get(odd_key)
        if val is not None and val != "":
            try:
                val_float = float(val)
                if val_float <= 1.0:
                    problemas.append(f"{odd_key} <= 1.0: {val_float}")
            except (ValueError, TypeError):
                problemas.append(f"{odd_key} não é numérico: {val}")
    
    # 4. Gols FT devem ser >= Gols HT
    for side in ["H", "A"]:
        ft_key = f"{side}_Gols_FT"
        ht_key = f"{side}_Gols_HT"
        ft = jogo.get(ft_key)
        ht = jogo.get(ht_key)
        if ft is not None and ht is not None:
            try:
                if int(ft) < int(ht):
                    problemas.append(f"{ft_key} ({ft}) < {ht_key} ({ht})")
            except (ValueError, TypeError):
                pass
    
    # 5. Gols não podem ser negativos
    for key in ["H_Gols_FT", "A_Gols_FT", "H_Gols_HT", "A_Gols_HT"]:
        val = jogo.get(key)
        if val is not None:
            try:
                if int(val) < 0:
                    problemas.append(f"{key} negativo: {val}")
            except (ValueError, TypeError):
                pass
    
    # 6. Data não pode estar vazia
    if not jogo.get("Data"):
        problemas.append("Data está vazia")
    
    # Log e strict mode
    if problemas:
        log.warning(f"Validação falhou para {home} vs {away}: {problemas}")
        if strict:
            raise MatchValidationError(
                f"Partida inválida ({home} vs {away}): {'; '.join(problemas)}"
            )
    
    return problemas


def validar_lote(jogos: list[dict]) -> dict:
    """
    Valida uma lista de partidas e retorna estatísticas.
    
    Returns:
        Dict com: total, validos, invalidos, problemas_por_tipo
    """
    total = len(jogos)
    invalidos = 0
    problemas_por_tipo = {}
    
    for jogo in jogos:
        problemas = validar_partida(jogo)
        if problemas:
            invalidos += 1
            for p in problemas:
                # Agrupa pelo tipo de problema (primeira palavra)
                tipo = p.split(":")[0].strip() if ":" in p else p
                problemas_por_tipo[tipo] = problemas_por_tipo.get(tipo, 0) + 1
    
    resultado = {
        "total": total,
        "validos": total - invalidos,
        "invalidos": invalidos,
        "taxa_validos": f"{((total - invalidos) / total * 100):.1f}%" if total > 0 else "N/A",
        "problemas_por_tipo": problemas_por_tipo
    }
    
    log.info(f"Validação de lote: {resultado['validos']}/{total} válidos ({resultado['taxa_validos']})")
    return resultado
