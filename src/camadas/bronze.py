"""
Bronze: guarda o que chegou, sem interpretar.

Structured Streaming com trigger availableNow lê só os arquivos novos e para sozinho. O checkpoint
garante que cada arquivo entre uma única vez, mesmo com o job rodando de novo. No Databricks, a
mesma função passa a usar Auto Loader trocando o formato para cloudFiles.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.config import Caminhos

ESQUEMA_LEITURAS = StructType([StructField(c, StringType()) for c in ["medidor_id", "ts", "energia", "firmware", "evento", "tensao_v"]])
ESQUEMA_SCADA = StructType([StructField(c, StringType()) for c in ["alimentador_id", "data_hora", "energia_kwh"]])


def _ingerir(spark: SparkSession, origem: str, destino: str, checkpoint: str, esquema: StructType, padrao: str) -> None:
    fluxo = (
        spark.readStream.schema(esquema).option("header", "true").option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true").option("pathGlobFilter", padrao).csv(origem)
        .withColumn("_arquivo", F.col("_metadata.file_path")).withColumn("_ingerido_em", F.current_timestamp())
    )
    consulta = fluxo.writeStream.format("delta").option("checkpointLocation", checkpoint).trigger(availableNow=True).start(destino)
    consulta.awaitTermination()


def ingerir(spark: SparkSession, c: Caminhos) -> None:
    _ingerir(spark, f"{c.landing}/leituras", c.bronze_leituras, f"{c.checkpoints}/bronze_leituras", ESQUEMA_LEITURAS, "*.csv.gz")
    _ingerir(spark, f"{c.landing}/scada", c.bronze_scada, f"{c.checkpoints}/bronze_scada", ESQUEMA_SCADA, "*.csv")
