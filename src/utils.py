"""Logger e formatação compartilhados."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)-10s | %(message)s"


def configurar_logger(nome: str = "pipeline") -> logging.Logger:
    logging.getLogger("py4j").setLevel(logging.WARNING)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=FORMATO, datefmt="%H:%M:%S")
    return logging.getLogger(nome)


def gravar_logs_em(caminho: Path) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    arquivo = logging.FileHandler(caminho, mode="w", encoding="utf-8")
    arquivo.setFormatter(logging.Formatter(FORMATO))
    logging.getLogger().addHandler(arquivo)


def formatar_br(valor, casas: int = 2) -> str:
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return ""
    if isinstance(valor, (bool, np.bool_)):
        return "sim" if valor else "não"
    if isinstance(valor, (int, np.integer)):
        return f"{valor:,}".replace(",", ".")
    if isinstance(valor, (float, np.floating)):
        return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return str(valor)


def df_para_markdown(df: pd.DataFrame, casas: int = 2) -> str:
    cabecalho = "| " + " | ".join(map(str, df.columns)) + " |"
    separador = "|" + "|".join("---" for _ in df.columns) + "|"
    linhas = ["| " + " | ".join("" if pd.isna(v) else formatar_br(v, casas) for v in linha) + " |"
              for linha in df.itertuples(index=False)]
    return "\n".join([cabecalho, separador, *linhas])
