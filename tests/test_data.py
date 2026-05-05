import pytest
from data import _normalizar, _formatar_data, _converter_stat_para_int

def test_normalizar():
    assert _normalizar("  São Paulo  ") == "sao paulo"
    assert _normalizar("Atlético-MG") == "atletico-mg"
    assert _normalizar("CRB/AL") == "crb/al"
    assert _normalizar(None) == ""
    assert _normalizar(123) == ""

def test_formatar_data():
    # Supondo que o formato de entrada seja compatível com pd.to_datetime
    assert _formatar_data("2023-10-05") == "2023-10-05"
    assert _formatar_data("10/05/2023") == "2023-10-05"
    assert _formatar_data("invalid-date") is None
    assert _formatar_data(None) is None

def test_converter_stat_para_int():
    assert _converter_stat_para_int("2-1") == [2, 1]
    assert _converter_stat_para_int(" 5 - 0 ") == [5, 0]
    assert _converter_stat_para_int("10") == [0, 0]
    assert _converter_stat_para_int("invalid") == [0, 0]
    assert _converter_stat_para_int(None) == [0, 0]
    assert _converter_stat_para_int("-") == [0, 0]
