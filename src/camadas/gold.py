"""Gold: tabelas de consumo. São pequenas perto da silver, então são reconstruídas a cada execução."""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import Caminhos
from src.transformacoes import gold as regras


def _gravar(df, destino: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(destino)


def executar(spark: SparkSession, c: Caminhos) -> None:
    ler = lambda caminho: spark.read.format("delta").load(caminho)
    intervalos, leituras, cadastro, scada = ler(c.silver_intervalos), ler(c.silver_leituras), ler(c.silver_medidores), ler(c.silver_scada)
    _gravar(regras.consumo_diario(intervalos), c.gold_consumo_diario)
    diario = ler(c.gold_consumo_diario)
    _gravar(regras.balanco_alimentador(diario, scada), c.gold_balanco_alimentador)
    balanco = ler(c.gold_balanco_alimentador)
    _gravar(regras.perfil_horario(intervalos), c.gold_perfil_horario)
    _gravar(regras.interrupcoes(leituras, cadastro, scada), c.gold_interrupcoes)
    _gravar(regras.suspeitas_de_fraude(diario, balanco), c.gold_suspeitas)
    _gravar(regras.faturamento_mensal(diario), c.gold_faturamento_mensal)
