"""Regras de negócio com poucas linhas: a vantagem de escrever funções puras de DataFrame."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.transformacoes import gold, leituras as regras

ESQUEMA_BRUTO = "medidor_id string, ts string, energia string, firmware string, evento string, tensao_v string, _arquivo string, _ingerido_em timestamp"
ESQUEMA_LEITURA = "medidor_id string, momento_utc timestamp, data date, leitura double, _arquivo string"


def _bruto(spark, *linhas):
    return spark.createDataFrame([(*l, "", "", "a.csv", datetime(2025, 1, 1)) for l in linhas], ESQUEMA_BRUTO)


def _momento(minuto):
    return datetime(2025, 1, 10, 12, 0) if minuto == 0 else datetime(2025, 1, 10, 12 + minuto // 60, minuto % 60)


def _leituras(spark, medidor, *valores):
    return spark.createDataFrame([(medidor, _momento(i * 15), date(2025, 1, 10), float(v), "a") for i, v in enumerate(valores)], ESQUEMA_LEITURA)


def _cadastro(spark, fator=1, inicial=0.0):
    return spark.createDataFrame([("M000001", "residencial", "AL001", fator, inicial)], "medidor_id string, classe string, alimentador_id string, fator_tc int, leitura_inicial double")


def _sem_trocas(spark):
    return spark.createDataFrame([], "medidor_id string, momento_troca timestamp, leitura_final double, leitura_inicial double")


def test_os_dois_firmwares_viram_o_mesmo_formato(spark):
    linhas = regras.normalizar(_bruto(spark, ("M1", "1736510400", "1500", "v1"), ("M2", "2025-01-10T09:00:00-03:00", "1.500", "v2"))).orderBy("medidor_id").collect()
    assert linhas[0]["momento_utc"] == linhas[1]["momento_utc"]
    assert linhas[0]["leitura"] == linhas[1]["leitura"] == 1.5


def test_relogio_atrasado_e_alinhado_aos_15_minutos(spark):
    linha = regras.normalizar(_bruto(spark, ("M1", "2025-01-10T09:00:35-03:00", "1", "v2"))).first()
    assert linha["momento_utc"].minute == 0 and linha["momento_utc"].second == 0


def test_leitura_da_meia_noite_pertence_ao_dia_anterior(spark):
    linha = regras.normalizar(_bruto(spark, ("M1", "2025-01-11T00:00:00-03:00", "1", "v2"))).first()
    assert linha["data"] == date(2025, 1, 10)


def test_valor_invalido_vai_para_a_quarentena_sem_derrubar_o_job(spark):
    df = regras.normalizar(_bruto(spark, ("M1", "2025-01-10T09:00:00-03:00", "abc", "v2"), ("#ERRO", None, None, None), ("M2", "ontem", "1", "v2")))
    validas, invalidas = regras.separar_invalidas(df)
    assert validas.count() == 0
    assert {r["motivo"] for r in invalidas.collect()} == {"energia_invalida", "linha_malformada", "data_invalida"}


def test_pico_isolado_e_removido(spark):
    limpas, picos = regras.remover_picos(_leituras(spark, "M1", 100, 101, 30000, 102, 103))
    assert [r["leitura"] for r in limpas.orderBy("momento_utc").collect()] == [100, 101, 102, 103]
    assert picos.count() == 1


def test_dois_picos_seguidos_sao_removidos(spark):
    limpas, picos = regras.remover_picos(_leituras(spark, "M1", 100, 101, 30000, 20000, 102, 103))
    assert limpas.count() == 4 and picos.count() == 2


def test_pico_na_ultima_leitura_e_removido(spark):
    limpas, _ = regras.remover_picos(_leituras(spark, "M1", 100, 101, 30000))
    assert limpas.count() == 2


def test_pico_na_primeira_leitura_do_trecho_usa_a_ancora(spark):
    ancora = spark.createDataFrame([("M1", datetime(2025, 1, 10, 11, 45), 99.0)], "medidor_id string, momento_utc timestamp, leitura double")
    limpas, picos = regras.remover_picos(_leituras(spark, "M1", 30000, 101, 102), ancora)
    assert picos.count() == 1 and limpas.count() == 2


def test_virada_de_registrador_nao_e_pico(spark):
    limpas, picos = regras.remover_picos(_leituras(spark, "M1", 99998.0, 99999.5, 0.5, 1.0))
    assert picos.count() == 0 and limpas.count() == 4


def _intervalos(spark, leituras, **kwargs):
    trocas = kwargs.pop("trocas", _sem_trocas(spark))
    return regras.calcular_intervalos(leituras, _cadastro(spark, **kwargs), trocas).orderBy("momento_utc").collect()


def test_energia_e_a_diferenca_entre_leituras_vezes_o_fator(spark):
    linhas = _intervalos(spark, _leituras(spark, "M000001", 10, 12, 15), fator=40, inicial=9.0)
    assert [r["energia_kwh"] for r in linhas] == [40.0, 80.0, 120.0]       # a primeira usa a leitura inicial do cadastro


def test_virada_soma_o_que_faltava_para_o_limite(spark):
    linhas = _intervalos(spark, _leituras(spark, "M000001", 99999.0, 1.0), inicial=99998.0)
    assert linhas[1]["situacao"] == "virada_de_registrador" and linhas[1]["energia_kwh"] == pytest.approx(2.0)


def test_troca_de_medidor_soma_os_dois_registradores(spark):
    trocas = spark.createDataFrame([("M000001", _momento(15), 60.0, 0.0)], "medidor_id string, momento_troca timestamp, leitura_final double, leitura_inicial double")
    linhas = _intervalos(spark, _leituras(spark, "M000001", 50, 58, 3), inicial=49.0, trocas=trocas)
    assert linhas[2]["situacao"] == "troca_de_medidor" and linhas[2]["energia_kwh"] == pytest.approx(5.0)


def test_buraco_nao_perde_energia_e_nao_gera_demanda_falsa(spark):
    df = _leituras(spark, "M000001", 10, 11, 12, 20).filter(F.col("leitura") != 11).filter(F.col("leitura") != 12)
    intervalos = regras.calcular_intervalos(df, _cadastro(spark, inicial=10.0), _sem_trocas(spark))
    diario = gold.consumo_diario(intervalos).first()
    assert diario["kwh"] == 10.0
    assert diario["demanda_maxima_kw"] == 0.0                              # o salto depois do buraco não vira potência


def test_ancora_liga_cargas_diferentes(spark):
    ancora = spark.createDataFrame([("M000001", datetime(2025, 1, 10, 11, 45), 7.0)], "medidor_id string, momento_utc timestamp, leitura double")
    linhas = regras.calcular_intervalos(_leituras(spark, "M000001", 10), _cadastro(spark, inicial=1.0), _sem_trocas(spark), ancora).collect()
    assert linhas[0]["energia_kwh"] == 3.0


def test_interrupcao_junta_intervalos_consecutivos_em_um_evento(spark):
    momentos = [_momento(m) for m in (0, 15, 30, 120)]
    scada = spark.createDataFrame([("AL1", m, 0.0) for m in momentos], "alimentador_id string, momento_utc timestamp, energia_kwh double")
    cadastro = spark.createDataFrame([("M1", "AL1"), ("M2", "AL1")], "medidor_id string, alimentador_id string")
    leituras = spark.createDataFrame([], "medidor_id string, momento_utc timestamp")
    eventos = gold.interrupcoes(leituras, cadastro, scada).orderBy("inicio_utc").collect()
    assert [e["intervalos"] for e in eventos] == [3, 1]
    assert eventos[0]["cliente_horas"] == pytest.approx(1.5)


def test_alimentador_sem_energia_com_medidores_respondendo_nao_e_interrupcao(spark):
    scada = spark.createDataFrame([("AL1", _momento(0), 0.0)], "alimentador_id string, momento_utc timestamp, energia_kwh double")
    cadastro = spark.createDataFrame([("M1", "AL1")], "medidor_id string, alimentador_id string")
    leituras = spark.createDataFrame([("M1", _momento(0))], "medidor_id string, momento_utc timestamp")
    assert gold.interrupcoes(leituras, cadastro, scada).count() == 0


def test_queda_do_medidor_gera_suspeita_mas_queda_da_classe_nao(spark):
    linhas = []
    for dia in range(1, 21):
        base = 10.0 if dia <= 10 else 5.0                                  # a classe inteira cai pela metade
        linhas += [("A", "AL1", "residencial", date(2025, 1, dia), base), ("B", "AL1", "residencial", date(2025, 1, dia), base), ("C", "AL1", "residencial", date(2025, 1, dia), base),
                   ("F", "AL1", "residencial", date(2025, 1, dia), 10.0 if dia <= 10 else 1.5)]
    diario = spark.createDataFrame(linhas, "medidor_id string, alimentador_id string, classe string, data date, kwh double")
    balanco = spark.createDataFrame([("AL1", 100.0, 100.0)], "alimentador_id string, perda_kwh double, kwh_injetado double")
    assert [r["medidor_id"] for r in gold.suspeitas_de_fraude(diario, balanco).collect()] == ["F"]
