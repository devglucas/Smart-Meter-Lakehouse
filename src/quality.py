"""Confere o lakehouse com a verdade do gerador e com identidades que precisam valer."""

from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import Caminhos


def _item(nome: str, ok: bool, detalhe: str, severidade: str = "critica") -> dict:
    return {"verificacao": nome, "severidade": severidade, "ok": bool(ok), "detalhe": detalhe}


def verificar(spark: SparkSession, c: Caminhos) -> tuple[list[dict], dict]:
    with open(f"{c.raiz}/gabarito.json", encoding="utf-8") as arquivo:
        g = json.load(arquivo)
    ler = lambda caminho: spark.read.format("delta").load(caminho)
    sujeira = g["sujeira"]
    silver, intervalos, quarentena = ler(c.silver_leituras), ler(c.silver_intervalos), ler(c.silver_quarentena)
    leituras = silver.count()
    motivos = dict(quarentena.groupBy("motivo").count().collect())
    bronze = ler(c.bronze_leituras).count()
    chaves_unicas = silver.select("medidor_id", "momento_utc").distinct().count()

    por_classe = dict(intervalos.groupBy("classe").agg(F.sum("energia_kwh")).collect())
    maior_erro = max(abs(por_classe[k] - v) / v for k, v in g["verdade"]["por_classe"].items())
    situacoes = dict(intervalos.groupBy("situacao").count().collect())

    suspeitos = {r[0] for r in ler(c.gold_suspeitas).select("medidor_id").collect()}
    fraudes = set(g["fraudes"])
    acertos = len(suspeitos & fraudes)
    precisao, recall = acertos / max(len(suspeitos), 1), acertos / max(len(fraudes), 1)

    eventos = {(r[0], str(r[1])) for r in ler(c.gold_interrupcoes).select("alimentador_id", F.to_date(F.from_utc_timestamp("inicio_utc", "America/Sao_Paulo"))).collect()}
    reais = {(e["alimentador_id"], e["data"]) for e in g["interrupcoes"]}
    achados = len(eventos & reais) / max(len(reais), 1)

    itens = [
        _item("gabarito: leituras únicas", leituras == sujeira["linhas"], f"{leituras} x {sujeira['linhas']}"),
        _item("silver sem chave repetida", chaves_unicas == leituras, f"{chaves_unicas} chaves"),
        _item("bronze guarda tudo o que chegou", bronze == sujeira["linhas"] + sujeira["duplicadas"] + sujeira["malformadas"], f"{bronze} linhas"),
        _item("gabarito: linhas malformadas", motivos.get("linha_malformada", 0) == sujeira["malformadas"], f"{motivos.get('linha_malformada', 0)} x {sujeira['malformadas']}"),
        _item("gabarito: picos na quarentena", motivos.get("pico_de_leitura", 0) == sujeira["picos"], f"{motivos.get('pico_de_leitura', 0)} x {sujeira['picos']}"),
        _item("gabarito: energia por classe", maior_erro < 1e-5, f"maior erro relativo: {maior_erro:.2e}"),
        _item("gabarito: trocas de medidor", situacoes.get("troca_de_medidor", 0) == g["trocas"], f"{situacoes.get('troca_de_medidor', 0)} x {g['trocas']}"),
        _item("nenhum retrocesso sem explicação", situacoes.get("retrocesso", 0) == 0, f"{situacoes.get('retrocesso', 0)} intervalos"),
        _item("gabarito: interrupções encontradas", achados >= 0.95, f"{achados:.1%} de {len(reais)} pares de alimentador e dia"),
        _item("fraudes: recall", recall >= 0.9, f"{recall:.1%}", "alerta"),
        _item("fraudes: precisão", precisao >= 0.8, f"{precisao:.1%}", "alerta"),
    ]
    return itens, {"fraudes_reais": len(fraudes), "suspeitos": len(suspeitos), "acertos": acertos, "precisao": precisao, "recall": recall, "motivos": motivos, "situacoes": situacoes,
                   "alimentadores_com_fraude": g["alimentadores_com_fraude"], "perda_tecnica_media": g["perda_tecnica_media"], "linhas_bronze": bronze, "leituras_silver": leituras}


def falhas_criticas(verificacoes: list[dict]) -> list[dict]:
    return [v for v in verificacoes if v["severidade"] == "critica" and not v["ok"]]
