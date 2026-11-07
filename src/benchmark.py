"""
Compara Pandas e PySpark fazendo o mesmo trabalho: dos arquivos brutos ao consumo diário.

O pai mede cada motor em um processo separado, com tempo e pico de memória somando os filhos,
porque o Spark roda em uma JVM. Se a memória passar do limite, o processo é encerrado e o
resultado é registrado como não concluído, em vez de travar a máquina.

    python -m src.benchmark --escalas pequena media grande
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

import psutil

from src.config import ROOT_DIR, caminhos
from src.camadas.bronze import ESQUEMA_LEITURAS

LIMITE_GB = 11.0


def rodar_pandas(raiz: str) -> None:
    from src import pandas_pipeline

    pandas_pipeline.executar(raiz).to_parquet(f"{raiz}/benchmark_pandas.parquet")


def rodar_spark(raiz: str) -> None:
    from pyspark.sql import functions as F

    from src import sessao
    from src.transformacoes import gold, leituras

    spark = sessao.criar()
    bruto = (spark.read.schema(ESQUEMA_LEITURAS).option("header", "true").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.csv.gz")
             .csv(f"{raiz}/raw/leituras").withColumn("_arquivo", F.lit("")).withColumn("_ingerido_em", F.current_timestamp()))
    validas, _ = leituras.separar_invalidas(leituras.normalizar(bruto))
    limpas, _ = leituras.remover_picos(validas.dropDuplicates(["medidor_id", "momento_utc"]))
    cadastro = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{raiz}/raw/cadastro/medidores.csv")
    trocas = (spark.read.option("header", "true").csv(f"{raiz}/raw/cadastro/trocas_medidor.csv")
              .select("medidor_id", F.to_timestamp("momento_utc").alias("momento_troca"), F.col("leitura_final").cast("double"), F.col("leitura_inicial").cast("double")))
    gold.consumo_diario(leituras.calcular_intervalos(limpas, cadastro, trocas)).write.mode("overwrite").parquet(f"{raiz}/benchmark_spark")


def medir(motor: str, escala: str) -> dict:
    processo = subprocess.Popen([sys.executable, "-m", "src.benchmark", "--filho", motor, "--escala", escala], cwd=ROOT_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    monitor, inicio, pico, concluiu = psutil.Process(processo.pid), time.perf_counter(), 0.0, True
    while processo.poll() is None:
        try:
            memoria = sum(p.memory_info().rss for p in [monitor, *monitor.children(recursive=True)]) / 1024 ** 3
        except psutil.Error:
            memoria = 0.0
        pico = max(pico, memoria)
        if pico > LIMITE_GB:
            for filho in monitor.children(recursive=True):
                filho.kill()
            processo.kill()
            concluiu = False
            break
        time.sleep(0.2)
    processo.wait()
    return {"motor": motor, "escala": escala, "segundos": round(time.perf_counter() - inicio, 1), "pico_gb": round(pico, 2), "concluiu": concluiu and processo.returncode == 0}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--escalas", nargs="+", default=["pequena"])
    parser.add_argument("--filho")
    parser.add_argument("--escala")
    argumentos = parser.parse_args()
    if argumentos.filho:
        {"pandas": rodar_pandas, "spark": rodar_spark}[argumentos.filho](caminhos(argumentos.escala).raiz)
        return
    resultados = []
    for escala in argumentos.escalas:
        for motor in ["pandas", "spark"]:
            resultados.append(medir(motor, escala))
            print(resultados[-1], flush=True)
    destino = ROOT_DIR / "output" / "reports" / "benchmark.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(resultados, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
