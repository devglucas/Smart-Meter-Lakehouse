"""Agregações e indicadores da camada gold, como funções puras."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.config import FUSO, TARIFAS


def consumo_diario(intervalos: DataFrame) -> DataFrame:
    potencia = F.when(F.col("minutos") <= 15, F.col("energia_kwh") * 4)      # depois de um buraco, o pico seria falso
    return intervalos.groupBy("medidor_id", "alimentador_id", "classe", "data").agg(
        F.sum("energia_kwh").alias("kwh"), F.count("*").alias("leituras"), F.max(potencia).alias("demanda_maxima_kw"))


def perfil_horario(intervalos: DataFrame) -> DataFrame:
    """Só intervalos regulares: uma diferença acumulada depois de um buraco distorceria a hora."""
    local = F.from_utc_timestamp("momento_utc", FUSO)
    tipo_dia = F.when(F.dayofweek(local).isin(1, 7), "fim_de_semana").otherwise("dia_util")
    medidores = intervalos.groupBy("classe").agg(F.countDistinct("medidor_id").alias("medidores"))
    dias = intervalos.withColumn("tipo_dia", tipo_dia).groupBy("tipo_dia").agg(F.countDistinct("data").alias("dias"))
    return (intervalos.filter("minutos = 15").withColumn("tipo_dia", tipo_dia).withColumn("hora", F.hour(local))
            .groupBy("classe", "tipo_dia", "hora").agg(F.sum("energia_kwh").alias("kwh"))
            .join(F.broadcast(medidores), "classe").join(F.broadcast(dias), "tipo_dia")
            .withColumn("kwh_por_medidor_hora", F.col("kwh") / F.col("medidores") / F.col("dias"))
            .select("classe", "tipo_dia", "hora", "kwh_por_medidor_hora"))


def balanco_alimentador(diario: DataFrame, scada: DataFrame) -> DataFrame:
    """Energia injetada no alimentador contra a medida nos clientes. A diferença é a perda."""
    medida = diario.groupBy("alimentador_id", "data").agg(F.sum("kwh").alias("kwh_medido"), F.countDistinct("medidor_id").alias("medidores"))
    injetada = scada.groupBy("alimentador_id", "data").agg(F.sum("energia_kwh").alias("kwh_injetado"))
    return (injetada.join(medida, ["alimentador_id", "data"])
            .withColumn("perda_kwh", F.col("kwh_injetado") - F.col("kwh_medido"))
            .withColumn("perda_pct", F.col("perda_kwh") / F.col("kwh_injetado")))


def interrupcoes(leituras: DataFrame, cadastro: DataFrame, scada: DataFrame) -> DataFrame:
    """
    Interrupção é o alimentador sem energia no SCADA e sem resposta dos medidores ao mesmo tempo.
    Os intervalos consecutivos viram um evento pela técnica de gaps and islands:
    momento menos o número da linha dá um valor constante dentro de cada sequência.
    """
    tamanho = cadastro.groupBy("alimentador_id").agg(F.count("*").alias("clientes"))
    respostas = (leituras.join(F.broadcast(cadastro.select("medidor_id", "alimentador_id")), "medidor_id")
                 .groupBy("alimentador_id", "momento_utc").agg(F.countDistinct("medidor_id").alias("responderam")))
    desligado = (scada.filter("energia_kwh = 0").select("alimentador_id", "momento_utc")
                 .join(respostas, ["alimentador_id", "momento_utc"], "left").join(F.broadcast(tamanho), "alimentador_id")
                 .filter(F.coalesce(F.col("responderam"), F.lit(0)) < F.col("clientes") * 0.2))
    janela = Window.partitionBy("alimentador_id").orderBy("momento_utc")
    ilhas = desligado.withColumn("ilha", F.unix_timestamp("momento_utc") - F.row_number().over(janela) * 900)
    return (ilhas.groupBy("alimentador_id", "ilha", "clientes")
            .agg(F.min("momento_utc").alias("inicio_utc"), F.count("*").alias("intervalos"))
            .withColumn("duracao_h", F.col("intervalos") / 4).withColumn("cliente_horas", F.col("duracao_h") * F.col("clientes")).drop("ilha"))


def suspeitas_de_fraude(diario: DataFrame, balanco: DataFrame, queda_minima: float = 0.5) -> DataFrame:
    """
    Compara cada medidor consigo mesmo. O consumo é dividido pela mediana da classe no dia, o que tira
    clima e fim de semana. Queda forte e sustentada entre o início e o fim do período gera suspeita.
    A perda do alimentador é a evidência independente: ela não vem do próprio medidor.
    """
    mediana = diario.groupBy("classe", "data").agg(F.percentile_approx("kwh", 0.5).alias("mediana"))
    limites = diario.agg(F.min("data").alias("primeiro"), F.max("data").alias("ultimo"))
    relativo = (diario.join(F.broadcast(mediana), ["classe", "data"]).crossJoin(F.broadcast(limites))
                .withColumn("indice", F.col("kwh") / F.col("mediana"))
                .withColumn("posicao", F.datediff("data", "primeiro") / F.datediff("ultimo", "primeiro")))
    resumo = relativo.groupBy("medidor_id", "alimentador_id", "classe").agg(
        F.percentile_approx(F.when(F.col("posicao") <= 0.2, F.col("indice")), 0.5).alias("indice_inicio"),
        F.percentile_approx(F.when(F.col("posicao") >= 0.8, F.col("indice")), 0.5).alias("indice_fim"),
        F.sum("kwh").alias("kwh_periodo"))
    perdas = balanco.groupBy("alimentador_id").agg((F.sum("perda_kwh") / F.sum("kwh_injetado")).alias("perda_alimentador"))
    return (resumo.withColumn("queda", 1 - F.col("indice_fim") / F.col("indice_inicio"))
            .filter(F.col("queda") >= queda_minima).join(F.broadcast(perdas), "alimentador_id"))


def faturamento_mensal(diario: DataFrame) -> DataFrame:
    tarifa = F.create_map(*[x for classe, valor in TARIFAS.items() for x in (F.lit(classe), F.lit(valor))])
    return (diario.withColumn("mes", F.trunc("data", "month")).groupBy("mes", "classe").agg(F.sum("kwh").alias("kwh"), F.countDistinct("medidor_id").alias("medidores"))
            .withColumn("receita", F.col("kwh") * tarifa[F.col("classe")]))
