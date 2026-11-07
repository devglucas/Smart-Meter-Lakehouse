"""
Gera a telemetria dos medidores inteligentes da Vale Claro Energia (empresa fictícia).

Uma leitura a cada 15 minutos, por medidor, de janeiro a março de 2025. O volume é escolhido pela escala:

    python scripts/generate_data.py --escala mini       60 medidores, 21 dias: testes
    python scripts/generate_data.py --escala pequena    300 medidores, 90 dias: cerca de 2,6 milhões de linhas
    python scripts/generate_data.py --escala media      2.000 medidores: cerca de 17 milhões de linhas
    python scripts/generate_data.py --escala grande     10.000 medidores: cerca de 86 milhões de linhas

Saídas em data/<escala>/:
    raw/leituras/dia=AAAA-MM-DD/parte-N.csv.gz   telemetria, na data em que chegou
    raw/scada/alimentador_AAAA-MM-DD.csv         energia injetada em cada alimentador
    raw/cadastro/medidores.csv e trocas_medidor.csv
    gabarito.json                                a verdade, antes da sujeira
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

RAIZ = Path(__file__).resolve().parents[1]
INICIO = date(2025, 1, 1)
INTERVALOS = 96
HORAS = 0.25
LIMITE_REGISTRADOR = 100_000.0
INICIO_TENSAO = date(2025, 3, 1)         # firmware novo passa a enviar a tensão
ESCALAS = {"mini": (60, 21, 3), "pequena": (300, 90, 6), "media": (2000, 90, 40), "grande": (10000, 90, 200)}
CLASSES = {"residencial": (0.82, 0.42), "comercial": (0.16, 3.2), "industrial": (0.02, 45.0)}   # participação e kW médio
FATORES_TC = [20, 40, 60]                # transformador de corrente: o registrador mostra energia dividida pelo fator

horas_do_dia = np.arange(INTERVALOS) / 4
PERFIS = {
    "residencial": 0.45 + 0.35 * np.exp(-((horas_do_dia - 7.5) / 1.5) ** 2) + 1.5 * np.exp(-((horas_do_dia - 19.5) / 2.2) ** 2),
    "comercial": 0.25 + 1.5 * ((horas_do_dia >= 8) & (horas_do_dia < 19)),
    "industrial": 0.75 + 0.45 * ((horas_do_dia >= 6) & (horas_do_dia < 22)),
}


def criar_cadastro(rng, medidores: int, alimentadores: int) -> pd.DataFrame:
    nomes = list(CLASSES)
    classe = rng.choice(nomes, medidores, p=[CLASSES[n][0] for n in nomes])
    media = np.array([CLASSES[c][1] for c in classe]) * rng.lognormal(0, 0.45, medidores)
    fator = np.where(classe == "industrial", rng.choice(FATORES_TC, medidores), 1)
    firmware = np.where(rng.random(medidores) < 0.55, "v1", "v2")
    leitura = rng.uniform(100, 90_000, medidores)
    perto_do_limite = (rng.random(medidores) < 0.01) & (classe == "residencial")
    leitura[perto_do_limite] = LIMITE_REGISTRADOR - rng.uniform(50, 300, perto_do_limite.sum())
    return pd.DataFrame({
        "medidor_id": [f"M{i:06d}" for i in range(1, medidores + 1)], "classe": classe, "alimentador_id": [f"AL{n:03d}" for n in rng.integers(1, alimentadores + 1, medidores)],
        "fator_tc": fator, "firmware": firmware, "kw_medio": media, "leitura_inicial": leitura.round(3),
    })


def texto_iso(momentos: np.ndarray) -> np.ndarray:
    """Hora local de Brasília com o deslocamento explícito, como o firmware novo envia."""
    return np.char.add(np.datetime_as_string(momentos - np.timedelta64(3, "h"), unit="s"), "-03:00")


def gerar(escala: str, semente: int = 9) -> dict:
    rng = np.random.default_rng(semente)
    medidores, dias, alimentadores = ESCALAS[escala]
    pasta = RAIZ / "data" / escala
    shutil.rmtree(pasta, ignore_errors=True)
    (pasta / "raw" / "scada").mkdir(parents=True)
    (pasta / "raw" / "cadastro").mkdir(parents=True)
    fim = INICIO + timedelta(days=dias - 1)

    cadastro = criar_cadastro(rng, medidores, alimentadores)
    n = len(cadastro)
    classes = cadastro["classe"].to_numpy()
    fatores = cadastro["fator_tc"].to_numpy().astype(float)
    kw_medio = cadastro["kw_medio"].to_numpy()
    perfil = np.vstack([PERFIS[c] for c in classes]) / np.vstack([PERFIS[c].mean() for c in classes])
    alim_ids = sorted(cadastro["alimentador_id"].unique())
    alim_idx = cadastro["alimentador_id"].map({a: i for i, a in enumerate(alim_ids)}).to_numpy()
    perda_tecnica = rng.uniform(0.05, 0.075, len(alim_ids))
    medidor_ids = cadastro["medidor_id"].to_numpy()

    # fraudes: a partir de um dia, o medidor registra só parte do consumo real
    # e se concentra em alguns alimentadores, como acontece em áreas com mais ligações irregulares
    quentes = rng.choice(len(alim_ids), max(1, len(alim_ids) // 8), replace=False)
    chance = np.where(np.isin(alim_idx, quentes), 0.22, 0.004) * (classes != "industrial")
    fraudes = np.where(rng.random(n) < chance)[0]
    inicio_fraude = np.full(n, 10_000)
    inicio_fraude[fraudes] = rng.integers(int(dias * 0.3), int(dias * 0.65), len(fraudes))

    # interrupções: o alimentador inteiro fica sem energia
    interrupcoes = []
    for a, alimentador in enumerate(alim_ids):
        for _ in range(int(rng.integers(1, 4))):
            dia, comeco, duracao = int(rng.integers(1, dias - 1)), int(rng.integers(0, 90)), int(rng.integers(3, 17))
            interrupcoes.append({"alimentador": a, "dia": dia, "inicio": comeco, "fim": min(comeco + duracao, INTERVALOS)})

    # troca de medidor: o registrador recomeça
    trocados = rng.choice(np.where(classes == "residencial")[0], max(1, int(n * 0.005)), replace=False)
    dia_troca = {int(i): int(rng.integers(5, dias - 3)) for i in trocados}
    trocas = []

    firmware_v2 = (cadastro["firmware"] == "v2").to_numpy()
    atualizados = (~firmware_v2) & (rng.random(n) < 0.3)
    dia_atualizacao = int(dias * 0.5)
    relogio_atrasado = rng.random(n) < 0.05

    registrador = cadastro["leitura_inicial"].to_numpy().copy()
    verdade = {"energia_medida_kwh": 0.0, "energia_real_kwh": 0.0, "por_classe": {c: 0.0 for c in CLASSES}}
    sujeira = {"linhas": 0, "duplicadas": 0, "atrasadas": 0, "picos": 0, "malformadas": 0, "perdidas_comunicacao": 0, "viradas_registrador": 0}
    pendentes: dict[date, list[pa.Table]] = {}
    pasta_leituras = pasta / "raw" / "leituras"
    colunas = ["medidor_id", "ts", "energia", "firmware", "evento", "tensao_v"]

    for d in range(dias):
        dia = INICIO + timedelta(days=d)
        fim_de_semana = dia.weekday() >= 5
        semana = np.where(classes == "residencial", 1.1 if fim_de_semana else 1.0, np.where(classes == "comercial", 0.35 if fim_de_semana else 1.0, 0.8 if fim_de_semana else 1.0))
        clima = 1 + 0.12 * np.sin(d / 9)
        potencia = (kw_medio * semana * clima)[:, None] * perfil * rng.gamma(20, 1 / 20, (n, INTERVALOS))
        real = potencia * HORAS

        sem_energia = np.zeros((n, INTERVALOS), dtype=bool)
        for evento in interrupcoes:
            if evento["dia"] == d:
                sem_energia[alim_idx == evento["alimentador"], evento["inicio"]:evento["fim"]] = True
        real[sem_energia] = 0

        medida = real.copy()
        fraudando = d >= inicio_fraude
        medida[fraudando] *= 0.3

        verdade["energia_real_kwh"] += float(real.sum())
        injetada = np.zeros((len(alim_ids), INTERVALOS))
        np.add.at(injetada, alim_idx, real)
        injetada /= (1 - perda_tecnica)[:, None]

        # registrador: acumula a energia dividida pelo fator do TC e vira em 100.000
        passos = np.cumsum(medida / fatores[:, None], axis=1)
        valores = registrador[:, None] + passos
        for i, dia_da_troca in dia_troca.items():
            if dia_da_troca == d:
                corte = int(rng.integers(30, 70))
                final = float(valores[i, corte - 1])
                valores[i, corte:] = passos[i, corte:] - passos[i, corte - 1]
                momento = np.datetime64(dia) + np.timedelta64(corte * 15 + 180, "m")
                trocas.append({"medidor_id": medidor_ids[i], "momento_utc": str(momento), "leitura_final": round(final, 3), "leitura_inicial": 0.0})
        registrador = valores[:, -1].copy()
        viradas = valores >= LIMITE_REGISTRADOR
        sujeira["viradas_registrador"] += int((viradas[:, -1] & (registrador < LIMITE_REGISTRADOR * 2)).sum())
        valores = np.where(viradas, valores - LIMITE_REGISTRADOR, valores)
        registrador = np.where(registrador >= LIMITE_REGISTRADOR, registrador - LIMITE_REGISTRADOR, registrador)

        cnt = medida.sum(axis=1)
        verdade["energia_medida_kwh"] += float(cnt.sum())
        for c in CLASSES:
            verdade["por_classe"][c] += float(cnt[classes == c].sum())

        # o que é transmitido: sem energia não há leitura; falha de comunicação perde leituras, mas o registrador segue
        transmitido = ~sem_energia
        falha = rng.random(n) < 0.01
        for i in np.where(falha)[0]:
            comeco = int(rng.integers(0, 90))
            transmitido[i, comeco:comeco + int(rng.integers(2, 7))] = False
        transmitido[:, -1] |= ~sem_energia[:, -1]            # o último do dia ancora o fechamento diário
        sujeira["perdidas_comunicacao"] += int((~transmitido & ~sem_energia).sum())

        fw2 = firmware_v2 | (atualizados & (d >= dia_atualizacao))
        linhas, cols = np.nonzero(transmitido)
        base = np.datetime64(dia) + np.timedelta64(3, "h")     # 00:00 local em UTC
        momento = base + ((cols + 1) * 15 * 60).astype("timedelta64[s]")
        atraso = np.where(relogio_atrasado[linhas], rng.integers(-40, 41, len(linhas)), 0).astype("timedelta64[s]")
        momento = momento + atraso
        energia = valores[linhas, cols]
        v2 = fw2[linhas]

        # pico: valor corrompido em uma leitura isolada
        picos = rng.random(len(energia)) < 0.00015
        sujeira["picos"] += int(picos.sum())
        energia = np.where(picos, energia + rng.uniform(5_000, 50_000, len(energia)), energia)

        epoch = momento.astype("datetime64[s]").astype(np.int64).astype(str)
        ts = np.where(v2, texto_iso(momento.astype("datetime64[s]")), epoch)
        texto_energia = np.where(v2, np.char.mod("%.3f", energia), np.char.mod("%.0f", energia * 1000))
        evento = np.where(rng.random(len(energia)) < 0.002, "ALARME_TENSAO", "")
        tensao = np.where(v2 & (dia >= INICIO_TENSAO), np.char.mod("%.1f", rng.normal(127, 3, len(energia))), "")
        tabela = pa.table([medidor_ids[linhas], ts, texto_energia, np.where(v2, "v2", "v1"), evento, tensao], names=colunas)
        sujeira["linhas"] += tabela.num_rows

        chegada = rng.random(tabela.num_rows)
        atrasadas = chegada < 0.02
        repetidas = chegada > 0.97                          # retransmissão: a mesma leitura vem de novo
        sujeira["atrasadas"] += int(atrasadas.sum())
        sujeira["duplicadas"] += int(repetidas.sum())
        no_dia = pa.concat_tables([tabela.filter(pa.array(~atrasadas)), tabela.filter(pa.array(repetidas))])
        pendentes.setdefault(dia, []).append(no_dia)
        atraso_dias = rng.integers(1, 4, atrasadas.sum())
        for extra in (1, 2, 3):
            selecao = np.where(atrasadas)[0][atraso_dias == extra]
            alvo = min(dia + timedelta(days=extra), fim)
            pendentes.setdefault(alvo, []).append(tabela.take(pa.array(selecao)))

        scada = pd.DataFrame({"alimentador_id": np.repeat(alim_ids, INTERVALOS),
                              "data_hora": np.tile([(pd.Timestamp(dia) + pd.Timedelta(minutes=15 * (k + 1))).strftime("%d/%m/%Y %H:%M") for k in range(INTERVALOS)], len(alim_ids)),
                              "energia_kwh": injetada.ravel().round(3)})
        scada.to_csv(pasta / "raw" / "scada" / f"alimentador_{dia}.csv", index=False)

        lote = pa.concat_tables(pendentes.pop(dia))
        destino = pasta_leituras / f"dia={dia}"
        destino.mkdir(parents=True)
        metade = lote.num_rows // 2
        for parte, pedaco in enumerate([lote.slice(0, metade), lote.slice(metade)]):
            caminho = destino / f"parte-{parte}.csv.gz"
            with pa.CompressedOutputStream(str(caminho), "gzip") as saida:
                pacsv.write_csv(pedaco, saida, pacsv.WriteOptions(quoting_style="none"))
            if parte == 0:
                ruins = max(1, pedaco.num_rows // 20_000)
                sujeira["malformadas"] += ruins
                with pa.CompressedOutputStream(str(destino / "parte-2.csv.gz"), "gzip") as extra:
                    extra.write((",".join(colunas) + "\n" + "#ERRO_TRANSMISSAO\n" * ruins).encode())

    cadastro.drop(columns=["kw_medio", "firmware"]).to_csv(pasta / "raw" / "cadastro" / "medidores.csv", index=False)
    pd.DataFrame(trocas, columns=["medidor_id", "momento_utc", "leitura_final", "leitura_inicial"]).to_csv(pasta / "raw" / "cadastro" / "trocas_medidor.csv", index=False)
    gabarito = {
        "escala": escala, "medidores": n, "dias": dias, "fim": fim.isoformat(), "fraudes": sorted(medidor_ids[fraudes].tolist()), "inicio_fraude_dia": {medidor_ids[i]: int(inicio_fraude[i]) for i in fraudes},
        "interrupcoes": [{"alimentador_id": alim_ids[e["alimentador"]], "data": (INICIO + timedelta(days=e["dia"])).isoformat(), "intervalos": e["fim"] - e["inicio"]} for e in interrupcoes],
        "alimentadores_com_fraude": [alim_ids[i] for i in quentes], "trocas": len(trocas), "verdade": verdade, "sujeira": sujeira, "perda_tecnica_media": float(perda_tecnica.mean()),
    }
    (pasta / "gabarito.json").write_text(json.dumps(gabarito, ensure_ascii=False, indent=2), encoding="utf-8")
    return gabarito


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--escala", choices=ESCALAS, default="pequena")
    argumentos = parser.parse_args()
    g = gerar(argumentos.escala)
    print({k: v for k, v in g.items() if k not in ("fraudes", "inicio_fraude_dia", "interrupcoes")}, len(g["fraudes"]), len(g["interrupcoes"]))


if __name__ == "__main__":
    main()
