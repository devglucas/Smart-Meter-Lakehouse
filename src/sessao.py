"""Sessão Spark. No Databricks ela já existe; localmente, é criada com o Delta Lake."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def criar(memoria: str = "6g", particoes: int = 16) -> SparkSession:
    ativa = SparkSession.getActiveSession()
    if ativa is not None:
        return ativa
    from delta import configure_spark_with_delta_pip

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    construtor = (
        SparkSession.builder.master("local[*]").appName("smart-meter-lakehouse")
        .config("spark.driver.memory", memoria)
        .config("spark.sql.shuffle.partitions", particoes)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", 4)
        .config("spark.ui.enabled", "false")
    )
    sessao = configure_spark_with_delta_pip(construtor).getOrCreate()
    sessao.sparkContext.setLogLevel("ERROR")
    return sessao
