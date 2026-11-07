"""Caminhos do lakehouse. Local usa data/<escala>; no Databricks, LAKE_RAIZ aponta para um Volume."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
FUSO = "America/Sao_Paulo"
LIMITE_REGISTRADOR = 100_000.0
SALTO_MAXIMO = 1_000.0        # acima disso entre duas leituras, o registrador não pode ter girado de verdade
TARIFAS = {"residencial": 0.78, "comercial": 0.82, "industrial": 0.61}       # R$/kWh


@dataclass(frozen=True)
class Caminhos:
    raiz: str

    def __getattr__(self, nome: str) -> str:
        pastas = {
            "raw": "raw", "landing": "landing", "checkpoints": "checkpoints",
            "bronze_leituras": "lake/bronze/leituras", "bronze_scada": "lake/bronze/scada",
            "silver_medidores": "lake/silver/medidores", "silver_trocas": "lake/silver/trocas",
            "silver_leituras": "lake/silver/leituras", "silver_intervalos": "lake/silver/intervalos",
            "silver_scada": "lake/silver/scada", "silver_quarentena": "lake/silver/quarentena", "silver_pendencias": "lake/silver/pendencias",
        }
        if nome in pastas:
            return f"{self.raiz}/{pastas[nome]}"
        if nome.startswith("gold_"):
            return f"{self.raiz}/lake/gold/{nome.removeprefix('gold_')}"
        raise AttributeError(nome)


def caminhos(escala: str) -> Caminhos:
    return Caminhos(os.environ.get("LAKE_RAIZ") or str(ROOT_DIR / "data" / escala))
