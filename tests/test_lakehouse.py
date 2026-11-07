"""Ponta a ponta na escala mini: gera os dados, passa pelos lotes e confere o lakehouse."""


import pandas as pd
import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

import main
from scripts import generate_data
from src import config, pandas_pipeline


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    generate_data.gerar("mini")
    saida = tmp_path_factory.mktemp("saida")
    return config.caminhos("mini"), main.executar("mini", lotes=3, saida=saida, recriar=True), saida


def _ler(spark, caminho):
    return spark.read.format("delta").load(caminho)


def test_verificacoes_criticas_passam(lake):
    _, resultado, _ = lake
    assert resultado["manifesto"]["sucesso"], [v for v in resultado["verificacoes"] if not v["ok"]]


def test_tres_lotes_viram_tres_merges(lake):
    _, resultado, _ = lake
    operacoes = [h["operacao"] for h in resultado["manifesto"]["historico_silver"]]
    assert operacoes.count("MERGE") == 3


def test_reexecutar_sem_arquivos_novos_nao_muda_nada(spark, lake, tmp_path):
    c, _, _ = lake
    antes = _ler(spark, c.bronze_leituras).count(), _ler(spark, c.silver_leituras).count(), _ler(spark, c.silver_quarentena).count()
    resultado = main.executar("mini", lotes=3, saida=tmp_path)
    assert resultado["manifesto"]["arquivos_novos"] == 0
    assert (_ler(spark, c.bronze_leituras).count(), _ler(spark, c.silver_leituras).count(), _ler(spark, c.silver_quarentena).count()) == antes


def test_time_travel_mostra_a_tabela_antes_do_ultimo_lote(spark, lake):
    c, _, _ = lake
    versao = [h["versao"] for h in DeltaTable.forPath(spark, c.silver_leituras).history().select(F.col("version").alias("versao"), "operation").filter("operation = 'MERGE'").orderBy("versao").collect()][-1]
    anterior = spark.read.format("delta").option("versionAsOf", versao - 1).load(c.silver_leituras).count()
    assert anterior < _ler(spark, c.silver_leituras).count()


def test_constraint_do_delta_recusa_leitura_negativa(spark, lake):
    c, _, _ = lake
    linha = _ler(spark, c.silver_leituras).limit(1).withColumn("leitura", F.lit(-1.0))
    with pytest.raises(Exception, match="leitura_nao_negativa|CHECK|violat"):
        linha.write.format("delta").mode("append").save(c.silver_leituras)


def test_schema_do_delta_recusa_tipo_errado(spark, lake):
    c, _, _ = lake
    linha = _ler(spark, c.silver_leituras).limit(1).withColumn("leitura", F.lit("texto"))
    with pytest.raises(Exception):
        linha.write.format("delta").mode("append").save(c.silver_leituras)


def test_pandas_e_pyspark_chegam_ao_mesmo_consumo_diario(spark, lake):
    c, _, _ = lake
    em_spark = _ler(spark, c.gold_consumo_diario).select("medidor_id", "data", "kwh").toPandas()
    em_pandas = pandas_pipeline.executar(c.raiz)[["medidor_id", "data", "kwh"]]
    juntos = em_spark.assign(data=pd.to_datetime(em_spark["data"])).merge(em_pandas.assign(data=pd.to_datetime(em_pandas["data"])), on=["medidor_id", "data"], how="outer", suffixes=("_spark", "_pandas"))
    assert juntos["kwh_spark"].notna().all() and juntos["kwh_pandas"].notna().all()
    assert (juntos["kwh_spark"] - juntos["kwh_pandas"]).abs().max() < 1e-6


def test_leitura_atrasada_recalcula_so_as_particoes_afetadas(spark, lake):
    c, _, _ = lake
    versoes = DeltaTable.forPath(spark, c.silver_intervalos).history().select("operationParameters").collect()
    predicados = [v["operationParameters"].get("predicate", "") for v in versoes]
    assert any("data >=" in p for p in predicados)


def test_saidas_foram_geradas(lake):
    _, _, saida = lake
    assert (saida / "relatorio.md").exists() and len(list((saida / "charts").glob("*.png"))) >= 3
