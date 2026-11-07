# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline medalhão
# MAGIC Executa uma camada. As regras ficam em `src/`, as mesmas que rodam localmente.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("camada", "bronze")
dbutils.widgets.text("lake_raiz", "/Volumes/main/energia/lakehouse")
os.environ["LAKE_RAIZ"] = dbutils.widgets.get("lake_raiz")
sys.path.append(os.path.abspath(".."))

# COMMAND ----------

from src import config
from src.camadas import bronze, gold, manutencao, silver

caminhos = config.caminhos("databricks")
camada = dbutils.widgets.get("camada")
{
    "bronze": lambda: bronze.ingerir(spark, caminhos),
    "silver": lambda: silver.executar(spark, caminhos),
    "gold": lambda: gold.executar(spark, caminhos),
    "manutencao": lambda: manutencao.otimizar(spark, caminhos),
}[camada]()
