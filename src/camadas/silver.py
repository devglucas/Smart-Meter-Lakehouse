"""
Silver: dado limpo, tipado e sem duplicados.

A leitura de bronze também é um fluxo: só o que entrou desde a última execução é processado.
Leitura atrasada entra pelo MERGE, e o intervalo é recalculado a partir do dia mais antigo afetado.
"""

from __future__ import annotations

from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Caminhos
from src.transformacoes import leituras as regras

COLUNAS_QUARENTENA = ["medidor_id", "momento_utc", "leitura", "motivo", "_arquivo"]


def existe(spark: SparkSession, caminho: str) -> bool:
    return DeltaTable.isDeltaTable(spark, caminho)


def atualizar_cadastro(spark: SparkSession, c: Caminhos) -> None:
    medidores = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{c.raw}/cadastro/medidores.csv")
    medidores.write.format("delta").mode("overwrite").save(c.silver_medidores)
    trocas = (spark.read.option("header", "true").csv(f"{c.raw}/cadastro/trocas_medidor.csv")
              .select("medidor_id", F.to_timestamp("momento_utc").alias("momento_troca"), F.col("leitura_final").cast("double"), F.col("leitura_inicial").cast("double")))
    trocas.write.format("delta").mode("overwrite").save(c.silver_trocas)


def atualizar_scada(spark: SparkSession, c: Caminhos) -> None:
    """O horário do SCADA é local, em texto. A tabela é pequena, então é reconstruída inteira."""
    momento = F.to_utc_timestamp(F.to_timestamp("data_hora", "dd/MM/yyyy HH:mm"), "America/Sao_Paulo")
    scada = (spark.read.format("delta").load(c.bronze_scada)
             .select("alimentador_id", momento.alias("momento_utc"), F.col("energia_kwh").cast("double"))
             .withColumn("data", regras.data_local(F.col("momento_utc"))).dropDuplicates(["alimentador_id", "momento_utc"]))
    scada.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(c.silver_scada)


def _quarentena(df: DataFrame, c: Caminhos) -> None:
    df.select(*COLUNAS_QUARENTENA).write.format("delta").mode("append").save(c.silver_quarentena)


def _criar_leituras(spark: SparkSession, lote: DataFrame, c: Caminhos) -> None:
    if not existe(spark, c.silver_leituras):
        lote.limit(0).write.format("delta").partitionBy("data").save(c.silver_leituras)
        spark.sql(f"ALTER TABLE delta.`{c.silver_leituras}` ADD CONSTRAINT leitura_nao_negativa CHECK (leitura >= 0)")


def _processar_lote(lote: DataFrame, identificador: int, c: Caminhos) -> None:
    """Roda em cada micro-lote. Usa a sessão do próprio lote, o que também funciona no Databricks serverless."""
    spark = lote.sparkSession
    validas, invalidas = regras.separar_invalidas(regras.normalizar(lote))
    _quarentena(invalidas, c)
    unicas = validas.dropDuplicates(["medidor_id", "momento_utc"])
    _criar_leituras(spark, unicas, c)
    menor, maior = unicas.agg(F.min("data"), F.max("data")).first()
    if menor is None:
        return
    # o filtro de data no MERGE poda partições: sem ele, a tabela inteira seria reescrita
    condicao = f"t.data BETWEEN '{menor}' AND '{maior}' AND t.medidor_id = s.medidor_id AND t.momento_utc = s.momento_utc AND t.data = s.data"
    DeltaTable.forPath(spark, c.silver_leituras).alias("t").merge(unicas.alias("s"), condicao).whenNotMatchedInsertAll().execute()
    # a pendência fica gravada: se o passo seguinte falhar, a próxima execução retoma daqui
    spark.createDataFrame([(menor,)], "menor_data date").write.format("delta").mode("append").save(c.silver_pendencias)


def atualizar_leituras(spark: SparkSession, c: Caminhos) -> None:
    consulta = (
        spark.readStream.format("delta").load(c.bronze_leituras).writeStream
        .foreachBatch(lambda lote, identificador: _processar_lote(lote, identificador, c))
        .option("checkpointLocation", f"{c.checkpoints}/silver_leituras").trigger(availableNow=True).start()
    )
    consulta.awaitTermination()


def _pendencia(spark: SparkSession, c: Caminhos):
    if not existe(spark, c.silver_pendencias):
        return None
    return spark.read.format("delta").load(c.silver_pendencias).agg(F.min("menor_data")).first()[0]


def _picos_conhecidos(spark: SparkSession, c: Caminhos) -> DataFrame:
    return spark.read.format("delta").load(c.silver_quarentena).filter("motivo = 'pico_de_leitura'").select("medidor_id", "momento_utc")


def atualizar_intervalos(spark: SparkSession, c: Caminhos) -> None:
    """
    Recalcula do dia anterior ao mais antigo afetado em diante e substitui só essas partições.
    O dia anterior entra porque um pico no fim do último lote só pode ser reconhecido com a leitura seguinte.
    """
    menor_data = _pendencia(spark, c)
    if menor_data is None:
        return
    data_inicial = menor_data - timedelta(days=1)
    leituras = spark.read.format("delta").load(c.silver_leituras)
    cadastro = spark.read.format("delta").load(c.silver_medidores)
    trocas = spark.read.format("delta").load(c.silver_trocas)
    conhecidos = _picos_conhecidos(spark, c) if existe(spark, c.silver_quarentena) else None

    trecho = leituras.filter(F.col("data") >= F.lit(data_inicial))
    anteriores = leituras.filter(F.col("data") < F.lit(data_inicial))
    if conhecidos is not None:
        anteriores = anteriores.join(conhecidos, ["medidor_id", "momento_utc"], "left_anti")
    ancoras = anteriores.groupBy("medidor_id").agg(F.max_by("leitura", "momento_utc").alias("leitura"), F.max("momento_utc").alias("momento_utc"))

    limpas, picos = regras.remover_picos(trecho.select("medidor_id", "momento_utc", "data", "leitura", "_arquivo"), ancoras)
    novos = picos if conhecidos is None else picos.join(conhecidos, ["medidor_id", "momento_utc"], "left_anti")
    _quarentena(novos.withColumn("motivo", F.lit("pico_de_leitura")), c)
    intervalos = regras.calcular_intervalos(limpas, cadastro, trocas, ancoras)
    escrita = intervalos.write.format("delta").mode("overwrite").partitionBy("data")
    if existe(spark, c.silver_intervalos):
        escrita = escrita.option("replaceWhere", f"data >= '{data_inicial}'")
    escrita.save(c.silver_intervalos)
    DeltaTable.forPath(spark, c.silver_pendencias).delete()


def executar(spark: SparkSession, c: Caminhos) -> None:
    atualizar_cadastro(spark, c)
    atualizar_scada(spark, c)
    atualizar_leituras(spark, c)
    atualizar_intervalos(spark, c)
