"""
A mesma lógica em Pandas: lê os arquivos, limpa, calcula os intervalos e consolida o consumo diário.

Serve para comparar resultado e custo. Pandas é mais simples e rápido enquanto os dados cabem na
memória; o limite é a RAM de uma máquina, porque tudo é carregado de uma vez.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.config import FUSO, LIMITE_REGISTRADOR, SALTO_MAXIMO

COLUNAS = ["medidor_id", "ts", "energia", "firmware"]


def ler_leituras(raiz: str) -> pd.DataFrame:
    arquivos = sorted((Path(raiz) / "raw" / "leituras").glob("dia=*/*.csv.gz"))
    return pd.concat([pd.read_csv(a, usecols=COLUNAS, dtype=str, on_bad_lines="skip") for a in arquivos], ignore_index=True)


def normalizar(df: pd.DataFrame) -> pd.DataFrame:
    v1 = df["firmware"].eq("v1").to_numpy()
    segundos = np.full(len(df), np.nan)
    segundos[v1] = pd.to_numeric(df.loc[v1, "ts"], errors="coerce")
    segundos[~v1] = pd.to_datetime(df.loc[~v1, "ts"], utc=True, errors="coerce", format="ISO8601").astype("datetime64[s, UTC]").astype("int64")
    energia = pd.to_numeric(df["energia"], errors="coerce").to_numpy()
    leitura = np.where(v1, energia / 1000, energia)
    alinhado = np.floor(segundos / 900 + 0.5) * 900
    limpo = pd.DataFrame({"medidor_id": df["medidor_id"].astype("category"), "segundos": alinhado, "leitura": leitura})
    limpo = limpo[limpo["medidor_id"].astype(str).str.startswith("M") & limpo["segundos"].notna() & limpo["leitura"].notna()]
    return limpo.drop_duplicates(["medidor_id", "segundos"]).sort_values(["medidor_id", "segundos"], kind="stable", ignore_index=True)


def remover_picos(df: pd.DataFrame, passadas: int = 2) -> pd.DataFrame:
    for _ in range(passadas):
        grupo = df.groupby("medidor_id", observed=True)["leitura"]
        anterior, seguinte = grupo.shift(1), grupo.shift(-1)
        pico = ((df["leitura"] > seguinte) & (anterior <= seguinte)) | (df["leitura"] - anterior > SALTO_MAXIMO)
        df = df[~pico].reset_index(drop=True)
    return df


def calcular_intervalos(df: pd.DataFrame, cadastro: pd.DataFrame, trocas: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(medidor_id=df["medidor_id"].astype(str)).merge(cadastro[["medidor_id", "classe", "alimentador_id", "fator_tc", "leitura_inicial"]], on="medidor_id")
    grupo = df.groupby("medidor_id")
    momento_anterior = grupo["segundos"].shift(1)
    anterior = grupo["leitura"].shift(1).fillna(df["leitura_inicial"])
    diferenca = df["leitura"] - anterior
    virada = (diferenca < 0) & (anterior > LIMITE_REGISTRADOR * 0.9) & (df["leitura"] < LIMITE_REGISTRADOR * 0.1)
    passo = np.where(virada, diferenca + LIMITE_REGISTRADOR, np.where(diferenca < 0, 0.0, diferenca))
    df = df.assign(passo=passo, minutos=((df["segundos"] - momento_anterior) / 60).fillna(15.0), anterior=anterior, momento_anterior=momento_anterior.fillna(0))
    for troca in trocas.itertuples():
        linha = (df["medidor_id"] == troca.medidor_id) & (df["momento_anterior"] <= troca.segundos) & (df["segundos"] > troca.segundos)
        df.loc[linha, "passo"] = np.maximum(troca.leitura_final - df.loc[linha, "anterior"], 0) + df.loc[linha, "leitura"] - troca.leitura_inicial
    momento = pd.to_datetime(df["segundos"] - 1, unit="s", utc=True).dt.tz_convert(FUSO)
    return df.assign(energia_kwh=df["passo"] * df["fator_tc"], data=momento.dt.date)[["medidor_id", "alimentador_id", "classe", "data", "energia_kwh", "minutos"]]


def consumo_diario(intervalos: pd.DataFrame) -> pd.DataFrame:
    potencia = (intervalos["energia_kwh"] * 4).where(intervalos["minutos"] <= 15)
    return (intervalos.assign(potencia=potencia).groupby(["medidor_id", "alimentador_id", "classe", "data"], as_index=False)
            .agg(kwh=("energia_kwh", "sum"), leituras=("energia_kwh", "size"), demanda_maxima_kw=("potencia", "max")))


def executar(raiz: str) -> pd.DataFrame:
    cadastro = pd.read_csv(Path(raiz) / "raw" / "cadastro" / "medidores.csv")
    trocas = pd.read_csv(Path(raiz) / "raw" / "cadastro" / "trocas_medidor.csv")
    trocas = trocas.assign(segundos=pd.to_datetime(trocas["momento_utc"], utc=True).astype("datetime64[s, UTC]").astype("int64"))
    return consumo_diario(calcular_intervalos(remover_picos(normalizar(ler_leituras(raiz))), cadastro, trocas))
