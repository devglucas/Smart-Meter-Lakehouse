"""
Simula a chegada dos arquivos: cada lote libera na landing os dias até uma data.

Usa links físicos, sem copiar os dados. Assim uma execução encontra só o que já "chegou",
como aconteceria com um coletor gravando em um bucket.
"""

from __future__ import annotations

import os
from pathlib import Path


def dias_disponiveis(raiz: str) -> list[str]:
    return sorted(p.name.removeprefix("dia=") for p in (Path(raiz) / "raw" / "leituras").iterdir())


def _ligar(origem: Path, destino: Path) -> int:
    if destino.exists():
        return 0
    destino.parent.mkdir(parents=True, exist_ok=True)
    os.link(origem, destino)
    return 1


def liberar(raiz: str, ate: str) -> int:
    base, novos = Path(raiz), 0
    for dia in dias_disponiveis(raiz):
        if dia > ate:
            continue
        for arquivo in (base / "raw" / "leituras" / f"dia={dia}").glob("*.csv.gz"):
            novos += _ligar(arquivo, base / "landing" / "leituras" / f"dia={dia}" / arquivo.name)
        scada = base / "raw" / "scada" / f"alimentador_{dia}.csv"
        novos += _ligar(scada, base / "landing" / "scada" / scada.name)
    return novos
