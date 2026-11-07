"""
Pipeline medalhão: landing -> bronze -> silver -> gold -> relatório, em lotes de chegada.

    python scripts/generate_data.py --escala pequena
    python main.py --escala pequena             processa o que ainda não foi processado
    python main.py --escala pequena --recriar   apaga o lakehouse e repassa todos os lotes
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src import chegada, config, quality, report, sessao, visualize
from src.camadas import bronze, gold, manutencao, silver
from src.utils import configurar_logger, gravar_logs_em

OUTPUT_DIR = config.ROOT_DIR / "output"
log = configurar_logger("main")


def cortes_dos_lotes(dias: list[str], lotes: int) -> list[str]:
    return [dias[min(len(dias) - 1, (i + 1) * len(dias) // lotes - (1 if i + 1 == lotes else 0))] for i in range(lotes)]


def executar(escala: str, lotes: int = 3, saida: Path = OUTPUT_DIR, recriar: bool = False) -> dict:
    inicio = time.perf_counter()
    c = config.caminhos(escala)
    if not Path(c.raw).exists():
        raise SystemExit(f"Dados não encontrados. Rode: python scripts/generate_data.py --escala {escala}")
    if recriar:
        for pasta in ("lake", "checkpoints", "landing"):
            shutil.rmtree(f"{c.raiz}/{pasta}", ignore_errors=True)

    spark = sessao.criar()
    arquivos_novos = 0
    for corte in cortes_dos_lotes(chegada.dias_disponiveis(c.raiz), lotes):
        novos = chegada.liberar(c.raiz, corte)
        arquivos_novos += novos
        if not novos:
            continue
        momento = time.perf_counter()
        bronze.ingerir(spark, c)
        silver.executar(spark, c)
        gold.executar(spark, c)
        log.info("lote até %s: %d arquivos em %.1fs", corte, novos, time.perf_counter() - momento)

    compactacao = manutencao.otimizar(spark, c)
    verificacoes, resumo = quality.verificar(spark, c)
    tabelas = report.ler_gold(spark, c)
    medidores = spark.read.format("delta").load(c.silver_medidores).count()
    kpis = report.indicadores(tabelas, resumo, medidores)
    report.exportar(tabelas, saida / "reports")
    report.escrever(tabelas, verificacoes, resumo, kpis, compactacao, saida / "relatorio.md")
    visualize.gerar(tabelas, resumo, saida / "charts", OUTPUT_DIR / "reports" / "benchmark.json")

    falhas = quality.falhas_criticas(verificacoes)
    manifesto = {
        "executado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"), "escala": escala, "sucesso": not falhas, "duracao_s": round(time.perf_counter() - inicio, 1),
        "arquivos_novos": arquivos_novos, "resumo": resumo, "indicadores": kpis, "compactacao": compactacao, "historico_silver": manutencao.historico(spark, c.silver_leituras),
        "verificacoes": {"total": len(verificacoes), "criticas_com_falha": len(falhas), "alertas": sum(v["severidade"] == "alerta" and not v["ok"] for v in verificacoes)},
    }
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "run_manifest.json").write_text(json.dumps(manifesto, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    for falha in falhas:
        log.error("verificação crítica falhou: %s (%s)", falha["verificacao"], falha["detalhe"])
    log.info("concluído em %.1fs | %d verificações, %d críticas com falha", manifesto["duracao_s"], len(verificacoes), len(falhas))
    return {"manifesto": manifesto, "verificacoes": verificacoes, "tabelas": tabelas}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--escala", default="pequena", choices=["mini", "pequena", "media", "grande"])
    parser.add_argument("--lotes", type=int, default=3)
    parser.add_argument("--recriar", action="store_true")
    argumentos = parser.parse_args()
    gravar_logs_em(OUTPUT_DIR / "logs" / "pipeline.log")
    resultado = executar(argumentos.escala, argumentos.lotes, recriar=argumentos.recriar)
    return 0 if resultado["manifesto"]["sucesso"] else 1


if __name__ == "__main__":
    sys.exit(main())
