"""Lê a camada gold e escreve as tabelas e o relatório."""

from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import Caminhos
from src.utils import df_para_markdown, formatar_br


def ler_gold(spark: SparkSession, c: Caminhos) -> dict[str, pd.DataFrame]:
    ler = lambda caminho: spark.read.format("delta").load(caminho)
    balanco = ler(c.gold_balanco_alimentador)
    tabelas = {
        "perfil_horario": ler(c.gold_perfil_horario),
        "faturamento_mensal": ler(c.gold_faturamento_mensal).orderBy("mes", "classe"),
        "interrupcoes": ler(c.gold_interrupcoes).orderBy("inicio_utc"),
        "suspeitas": ler(c.gold_suspeitas).orderBy(F.desc("queda")),
        "perda_por_alimentador": balanco.groupBy("alimentador_id").agg(F.sum("kwh_injetado").alias("kwh_injetado"), F.sum("kwh_medido").alias("kwh_medido"), F.max("medidores").alias("medidores"))
        .withColumn("perda_pct", 1 - F.col("kwh_medido") / F.col("kwh_injetado")).orderBy(F.desc("perda_pct")),
        "perda_semanal": balanco.withColumn("semana", F.date_trunc("week", "data").cast("date")).groupBy("alimentador_id", "semana")
        .agg(F.sum("kwh_injetado").alias("kwh_injetado"), F.sum("kwh_medido").alias("kwh_medido")).withColumn("perda_pct", 1 - F.col("kwh_medido") / F.col("kwh_injetado")).orderBy("alimentador_id", "semana"),
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return {nome: df.toPandas() for nome, df in tabelas.items()}


def exportar(tabelas: dict[str, pd.DataFrame], pasta: Path) -> None:
    pasta.mkdir(parents=True, exist_ok=True)
    for nome, tabela in tabelas.items():
        tabela.to_csv(pasta / f"{nome}.csv", index=False, float_format="%.4f")


def indicadores(t: dict[str, pd.DataFrame], resumo: dict, medidores: int) -> dict:
    perdas = t["perda_por_alimentador"]
    quentes = perdas["alimentador_id"].isin(resumo["alimentadores_com_fraude"])
    faturamento = t["faturamento_mensal"]
    eventos = t["interrupcoes"]
    return {
        "kwh_medido": float(perdas["kwh_medido"].sum()), "kwh_injetado": float(perdas["kwh_injetado"].sum()), "receita": float(faturamento["receita"].sum()),
        "perda_geral": float(1 - perdas["kwh_medido"].sum() / perdas["kwh_injetado"].sum()),
        "perda_alimentadores_com_fraude": float(1 - perdas.loc[quentes, "kwh_medido"].sum() / perdas.loc[quentes, "kwh_injetado"].sum()),
        "perda_demais": float(1 - perdas.loc[~quentes, "kwh_medido"].sum() / perdas.loc[~quentes, "kwh_injetado"].sum()),
        "interrupcoes": len(eventos), "dec_horas": float(eventos["cliente_horas"].sum() / medidores), "fec": float(eventos["clientes"].sum() / medidores),
    }


def escrever(t: dict[str, pd.DataFrame], verificacoes: list[dict], resumo: dict, kpis: dict, manutencao: dict, destino: Path) -> None:
    tabela_v = pd.DataFrame(verificacoes).assign(resultado=lambda d: d["ok"].map({True: "ok", False: "falhou"}))
    partes = [
        "# Relatório de telemetria: Vale Claro Energia\n", "## Resumo\n",
        f"- Leituras na silver: {formatar_br(resumo['leituras_silver'])}",
        f"- Energia medida: {formatar_br(kpis['kwh_medido'], 0)} kWh",
        f"- Perda geral: {formatar_br(kpis['perda_geral'] * 100, 1)}%",
        f"- Suspeitas de fraude: {resumo['suspeitos']} (precisão de {formatar_br(resumo['precisao'] * 100, 1)}%, recall de {formatar_br(resumo['recall'] * 100, 1)}%)",
        f"- Interrupções: {kpis['interrupcoes']}, com DEC de {formatar_br(kpis['dec_horas'])} horas e FEC de {formatar_br(kpis['fec'])}\n",
        "## Perda por alimentador\n", df_para_markdown(t["perda_por_alimentador"].head(12), 3), "",
        "## Faturamento mensal\n", df_para_markdown(t["faturamento_mensal"].assign(mes=lambda d: d["mes"].astype(str).str[:7])), "",
        f"## Compactação da silver\n\nArquivos: {manutencao['antes']['arquivos']} antes e {manutencao['depois']['arquivos']} depois. "
        f"Consulta de um medidor: {manutencao['antes']['consulta_s']:.2f}s antes e {manutencao['depois']['consulta_s']:.2f}s depois.\n",
        "## Verificações\n", df_para_markdown(tabela_v[["verificacao", "severidade", "resultado", "detalhe"]]), "",
    ]
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text("\n".join(partes), encoding="utf-8")
