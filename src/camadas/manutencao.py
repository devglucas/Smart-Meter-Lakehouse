"""
Manutenção do Delta: arquivos pequenos são o custo de cargas frequentes.

OPTIMIZE compacta os arquivos e ZORDER agrupa um medidor em poucos deles, para a consulta
pular o resto. O efeito é medido antes e depois, em vez de assumido.
"""

from __future__ import annotations

import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import Caminhos


def _arquivos(spark: SparkSession, caminho: str) -> int:
    return spark.sql(f"DESCRIBE DETAIL delta.`{caminho}`").first()["numFiles"]


def _consulta_de_um_medidor(spark: SparkSession, caminho: str, medidor: str) -> float:
    tempos = []
    for _ in range(3):
        inicio = time.perf_counter()
        spark.read.format("delta").load(caminho).filter(F.col("medidor_id") == medidor).agg(F.sum("leitura")).first()
        tempos.append(time.perf_counter() - inicio)
    return sorted(tempos)[1]


def otimizar(spark: SparkSession, c: Caminhos) -> dict:
    medidor = spark.read.format("delta").load(c.silver_medidores).first()["medidor_id"]
    antes = {"arquivos": _arquivos(spark, c.silver_leituras), "consulta_s": _consulta_de_um_medidor(spark, c.silver_leituras, medidor)}
    DeltaTable.forPath(spark, c.silver_leituras).optimize().executeZOrderBy("medidor_id")
    depois = {"arquivos": _arquivos(spark, c.silver_leituras), "consulta_s": _consulta_de_um_medidor(spark, c.silver_leituras, medidor)}
    return {"antes": antes, "depois": depois}


def historico(spark: SparkSession, caminho: str) -> list[dict]:
    linhas = DeltaTable.forPath(spark, caminho).history().select("version", "operation", "operationMetrics").orderBy("version").collect()
    return [{"versao": l["version"], "operacao": l["operation"], "linhas_inseridas": (l["operationMetrics"] or {}).get("numTargetRowsInserted")} for l in linhas]
