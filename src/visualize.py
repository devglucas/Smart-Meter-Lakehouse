"""Gráficos do relatório."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import matplotlib
import matplotlib.dates

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.getLogger("matplotlib").setLevel(logging.WARNING)

SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
CINZA, TINTA_2, GRADE, EIXO, SUPERFICIE = "#b5b3ab", "#52514e", "#e1e0d9", "#c3c2b7", "#fcfcfb"
MESES = ["jan", "fev", "mar"]
NOMES_CLASSE = {"residencial": "Residencial", "comercial": "Comercial", "industrial": "Industrial"}


def _estilo() -> None:
    plt.rcParams.update({
        "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "savefig.facecolor": SUPERFICIE, "font.size": 11,
        "axes.edgecolor": EIXO, "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
        "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRADE,
        "axes.axisbelow": True, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlepad": 24, "axes.titlelocation": "left",
    })


def _salvar(fig, pasta: Path, nome: str) -> None:
    fig.tight_layout()
    fig.savefig(pasta / f"{nome}.png", dpi=150)
    plt.close(fig)


def perfil(tabela: pd.DataFrame, pasta: Path) -> None:
    fig, eixos = plt.subplots(1, 3, figsize=(11, 4), sharex=True)
    dados = tabela[tabela["tipo_dia"] == "dia_util"]
    for ax, (classe, nome), cor in zip(eixos, NOMES_CLASSE.items(), SERIES):
        grupo = dados[dados["classe"] == classe].sort_values("hora")
        ax.plot(grupo["hora"], grupo["kwh_por_medidor_hora"], color=cor, linewidth=2.2)
        ax.set_title(nome, pad=8, fontsize=11)
        ax.set_ylim(0, None)
        ax.set_xticks([0, 6, 12, 18, 23])
        ax.set_xlabel("Hora local")
    eixos[0].set_ylabel("kWh por medidor")
    fig.suptitle("Cada classe tem seu horário de pico em um dia útil", x=0.02, ha="left", fontsize=13, fontweight="bold")
    _salvar(fig, pasta, "01_perfil_horario")


def perda_alimentadores(tabela: pd.DataFrame, quentes: list[str], tecnica: float, pasta: Path) -> None:
    dados = tabela.sort_values("perda_pct", ascending=False).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(10, 4.8))
    cores = [SERIES[1] if a in quentes else CINZA for a in dados["alimentador_id"]]
    ax.bar(dados.index, dados["perda_pct"] * 100, color=cores, width=0.8)
    ax.axhline(tecnica * 100, color=TINTA_2, linewidth=1)
    ax.text(len(dados) - 0.5, tecnica * 100 + 0.25, f"perda técnica média: {tecnica * 100:.1f}%", ha="right", color=TINTA_2, fontsize=9)
    ax.set_xticks([])
    ax.set_xlabel("Alimentadores, do maior para o menor")
    ax.set_ylabel("Perda de energia (%)")
    ax.set_title("A perda acima do esperado se concentra em poucos alimentadores (em laranja)")
    _salvar(fig, pasta, "02_perda_por_alimentador")


def perda_no_tempo(semanal: pd.DataFrame, quentes: list[str], pasta: Path) -> None:
    dados = semanal.assign(grupo=semanal["alimentador_id"].isin(quentes)).groupby(["grupo", "semana"], as_index=False)[["kwh_injetado", "kwh_medido"]].sum()
    dados["perda"] = (1 - dados["kwh_medido"] / dados["kwh_injetado"]) * 100
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    for grupo, nome, cor in [(True, "Alimentadores com fraude", SERIES[1]), (False, "Demais alimentadores", SERIES[0])]:
        linha = dados[dados["grupo"] == grupo].iloc[1:-1]
        ax.plot(pd.to_datetime(linha["semana"]), linha["perda"], color=cor, linewidth=2.2, label=nome)
    ax.set_ylim(0, None)
    ax.set_ylabel("Perda semanal (%)")
    ax.set_title("A perda quase triplica nos alimentadores com fraude")
    ax.legend(frameon=False, ncols=2, loc="lower left", bbox_to_anchor=(0, 1.0), borderaxespad=0, fontsize=10)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%d/%m"))
    _salvar(fig, pasta, "03_perda_no_tempo")


def benchmark(caminho: Path, pasta: Path) -> None:
    if not caminho.exists():
        return
    dados = pd.DataFrame(json.loads(caminho.read_text(encoding="utf-8")))
    escalas = list(dict.fromkeys(dados["escala"]))
    rotulos = {"pequena": "2,6 milhões", "media": "17 milhões", "grande": "86 milhões"}
    fig, eixos = plt.subplots(1, 2, figsize=(11, 4.6))
    posicoes, largura = np.arange(len(escalas)), 0.36
    for ax, coluna, titulo in [(eixos[0], "segundos", "Tempo (segundos)"), (eixos[1], "pico_gb", "Pico de memória (GB)")]:
        for i, (motor, nome, cor) in enumerate([("pandas", "Pandas", SERIES[1]), ("spark", "PySpark", SERIES[0])]):
            grupo = dados[dados["motor"] == motor].set_index("escala").loc[escalas]
            barras = ax.bar(posicoes + (i - 0.5) * largura, grupo[coluna], largura - 0.02, color=cor, label=nome)
            for barra, concluiu in zip(barras, grupo["concluiu"]):
                texto = f"{barra.get_height():.0f}" if coluna == "segundos" else f"{barra.get_height():.1f}"
                ax.annotate(texto if concluiu else "não\nconcluiu", (barra.get_x() + barra.get_width() / 2, barra.get_height()), textcoords="offset points", xytext=(0, 3), ha="center", fontsize=9, color=TINTA_2)
        ax.set_xticks(posicoes, [rotulos.get(e, e) for e in escalas])
        ax.set_xlabel("Linhas processadas")
        ax.set_title(titulo, pad=8, fontsize=11)
        ax.margins(y=0.18)
    eixos[0].legend(frameon=False)
    fig.suptitle("Pandas vence no volume pequeno; o PySpark ganha quando o volume cresce", x=0.02, ha="left", fontsize=13, fontweight="bold")
    _salvar(fig, pasta, "04_pandas_vs_pyspark")


def gerar(t: dict[str, pd.DataFrame], resumo: dict, pasta: Path, arquivo_benchmark: Path) -> None:
    pasta.mkdir(parents=True, exist_ok=True)
    _estilo()
    perfil(t["perfil_horario"], pasta)
    perda_alimentadores(t["perda_por_alimentador"], resumo["alimentadores_com_fraude"], resumo["perda_tecnica_media"], pasta)
    perda_no_tempo(t["perda_semanal"], resumo["alimentadores_com_fraude"], pasta)
    benchmark(arquivo_benchmark, pasta)
