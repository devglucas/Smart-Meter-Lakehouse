"""
Regras de negócio da telemetria, como funções puras de DataFrame para DataFrame.

Nenhuma função aqui lê ou escreve: isso permite testar com poucas linhas e rodar igual
no notebook do Databricks. Tudo usa funções nativas do Spark, sem UDF em Python.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.config import FUSO, LIMITE_REGISTRADOR, SALTO_MAXIMO

INTERVALO_S = 900


def normalizar(bruto: DataFrame) -> DataFrame:
    """
    Unifica os dois firmwares: v1 envia epoch e energia em Wh; v2 envia ISO com fuso e kWh.
    O relógio de alguns medidores atrasa, então o horário é arredondado para os 15 minutos mais próximos.
    As conversões usam try_*: valor inválido vira nulo e vai para a quarentena, em vez de derrubar o job.
    """
    momento = F.when(F.col("firmware") == "v1", F.timestamp_seconds(F.col("ts").try_cast("long"))).otherwise(F.try_to_timestamp(F.col("ts")))
    energia = F.col("energia").try_cast("double")
    leitura = F.when(F.col("firmware") == "v1", energia / 1000).otherwise(energia)
    segundos = F.unix_timestamp(momento)
    alinhado = F.timestamp_seconds(F.round(segundos / INTERVALO_S) * INTERVALO_S)
    return bruto.select(
        "medidor_id", alinhado.alias("momento_utc"), leitura.alias("leitura"), "firmware", F.nullif(F.col("evento"), F.lit("")).alias("evento"),
        F.col("tensao_v").try_cast("double").alias("tensao_v"), "_arquivo", "_ingerido_em",
    ).withColumn("data", data_local(F.col("momento_utc")))


def data_local(momento) -> F.Column:
    """A leitura marca o fim do intervalo: a de 00:00 pertence ao dia anterior."""
    return F.to_date(F.from_utc_timestamp(momento - F.expr("INTERVAL 1 SECOND"), FUSO))


def separar_invalidas(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    motivo = (F.when(F.col("medidor_id").isNull() | ~F.col("medidor_id").startswith("M"), "linha_malformada")
              .when(F.col("momento_utc").isNull(), "data_invalida")
              .when(F.col("leitura").isNull() | (F.col("leitura") < 0), "energia_invalida"))
    marcado = df.withColumn("motivo", motivo)
    return marcado.filter("motivo IS NULL").drop("motivo"), marcado.filter("motivo IS NOT NULL")


def remover_picos(leituras: DataFrame, ancoras: DataFrame | None = None, passadas: int = 2) -> tuple[DataFrame, DataFrame]:
    """
    Registrador só cresce. Valor maior que o seguinte, com o anterior abaixo do seguinte, é corrompido.
    Virada e troca não caem aqui: nelas o valor anterior é maior que o seguinte.
    A última leitura de um medidor não tem seguinte, então vale também um limite físico de salto.

    A âncora dá à primeira leitura do trecho um valor anterior para comparar. São duas passadas
    porque, com dois picos seguidos, o segundo só aparece depois que o primeiro sai.
    """
    janela = Window.partitionBy("medidor_id").orderBy("momento_utc")
    restante, removidas = leituras.withColumn("_ancora", F.lit(False)), None
    if ancoras is not None:
        extras = ancoras.select("medidor_id", "momento_utc", F.lit(None).cast("date").alias("data"), "leitura", F.lit(None).cast("string").alias("_arquivo"), F.lit(True).alias("_ancora"))
        restante = restante.unionByName(extras)
    for _ in range(passadas):
        anterior, seguinte = F.lag("leitura").over(janela), F.lead("leitura").over(janela)
        pico = ((F.col("leitura") > seguinte) & (anterior <= seguinte)) | (F.col("leitura") - anterior > SALTO_MAXIMO)
        marcado = restante.withColumn("pico", F.coalesce(pico, F.lit(False)) & ~F.col("_ancora"))
        achados = marcado.filter("pico").drop("pico")
        removidas = achados if removidas is None else removidas.unionByName(achados)
        restante = marcado.filter("NOT pico").drop("pico")
    return restante.filter("NOT _ancora").drop("_ancora"), removidas.drop("_ancora")


def calcular_intervalos(leituras: DataFrame, cadastro: DataFrame, trocas: DataFrame, ancoras: DataFrame | None = None) -> DataFrame:
    """
    Energia de cada intervalo, pela diferença entre leituras consecutivas do registrador.

    Uma diferença negativa tem três causas: troca do medidor, virada em 100.000 ou erro.
    A âncora é a última leitura conhecida antes do trecho, para a conta atravessar cargas diferentes.
    Depois de uma falha de comunicação, a diferença cobre todo o período sem leitura,
    então nenhuma energia se perde; ela só fica concentrada na leitura seguinte.
    """
    janela = Window.partitionBy("medidor_id").orderBy("momento_utc")
    base = leituras.select("medidor_id", "momento_utc", "data", "leitura")
    if ancoras is not None:
        base = base.unionByName(ancoras.select("medidor_id", "momento_utc", F.lit(None).cast("date").alias("data"), "leitura"))
    com_anterior = (
        base.withColumn("anterior", F.lag("leitura").over(janela))
        .withColumn("momento_anterior", F.lag("momento_utc").over(janela))
        .filter("data IS NOT NULL")
        .join(F.broadcast(cadastro.select("medidor_id", "classe", "alimentador_id", "fator_tc", "leitura_inicial")), "medidor_id")
    )
    anterior = F.coalesce(F.col("anterior"), F.col("leitura_inicial"))
    troca = F.broadcast(trocas.select("medidor_id", "momento_troca", "leitura_final", F.col("leitura_inicial").alias("leitura_nova")))
    condicao = ((com_anterior["medidor_id"] == troca["medidor_id"]) & (troca["momento_troca"] >= F.coalesce(com_anterior["momento_anterior"], F.lit("2000-01-01").cast("timestamp")))
                & (troca["momento_troca"] < com_anterior["momento_utc"]))
    unido = com_anterior.join(troca, condicao, "left").drop(troca["medidor_id"])

    diferenca = F.col("leitura") - anterior
    situacao = (F.when(F.col("momento_troca").isNotNull(), "troca_de_medidor")
                .when((diferenca < 0) & (anterior > LIMITE_REGISTRADOR * 0.9) & (F.col("leitura") < LIMITE_REGISTRADOR * 0.1), "virada_de_registrador")
                .when(diferenca < 0, "retrocesso")
                .otherwise("normal"))
    passo = (F.when(F.col("situacao") == "troca_de_medidor", F.greatest(F.col("leitura_final") - anterior, F.lit(0.0)) + F.col("leitura") - F.col("leitura_nova"))
             .when(F.col("situacao") == "virada_de_registrador", diferenca + LIMITE_REGISTRADOR)
             .when(F.col("situacao") == "retrocesso", F.lit(0.0))
             .otherwise(diferenca))
    minutos = (F.unix_timestamp("momento_utc") - F.unix_timestamp("momento_anterior")) / 60
    return unido.withColumn("situacao", situacao).select(
        "medidor_id", "alimentador_id", "classe", "momento_utc", "data", (passo * F.col("fator_tc")).alias("energia_kwh"), F.coalesce(minutos, F.lit(15.0)).alias("minutos"), "situacao",
    )
