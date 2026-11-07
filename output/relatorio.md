# Relatório de telemetria: Vale Claro Energia

## Resumo

- Leituras na silver: 17.235.095
- Energia medida: 7.273.881 kWh
- Perda geral: 6,7%
- Suspeitas de fraude: 69 (precisão de 100,0%, recall de 100,0%)
- Interrupções: 80, com DEC de 4,71 horas e FEC de 2,00

## Perda por alimentador

| alimentador_id | kwh_injetado | kwh_medido | medidores | perda_pct |
|---|---|---|---|---|
| AL018 | 81.926,917 | 66.721,755 | 50 | 0,186 |
| AL009 | 116.255,077 | 96.782,878 | 48 | 0,167 |
| AL017 | 110.309,628 | 94.432,164 | 55 | 0,144 |
| AL020 | 240.186,364 | 209.901,743 | 60 | 0,126 |
| AL033 | 152.405,577 | 137.629,681 | 37 | 0,097 |
| AL035 | 482.537,143 | 446.423,218 | 48 | 0,075 |
| AL021 | 237.838,095 | 220.123,987 | 55 | 0,074 |
| AL003 | 291.997,709 | 270.279,111 | 48 | 0,074 |
| AL031 | 93.974,371 | 87.214,936 | 45 | 0,072 |
| AL038 | 251.784,285 | 234.189,170 | 60 | 0,070 |
| AL012 | 204.199,370 | 190.306,989 | 57 | 0,068 |
| AL040 | 247.853,789 | 231.074,276 | 46 | 0,068 |

## Faturamento mensal

| mes | classe | kwh | medidores | receita |
|---|---|---|---|---|
| 2025-01 | comercial | 714.096,32 | 298 | 585.558,98 |
| 2025-01 | industrial | 1.316.636,96 | 34 | 803.148,55 |
| 2025-01 | residencial | 628.658,60 | 1.668 | 490.353,71 |
| 2025-02 | comercial | 539.717,50 | 298 | 442.568,35 |
| 2025-02 | industrial | 1.023.846,12 | 34 | 624.546,13 |
| 2025-02 | residencial | 486.106,35 | 1.668 | 379.162,95 |
| 2025-03 | comercial | 660.166,23 | 298 | 541.336,31 |
| 2025-03 | industrial | 1.290.757,16 | 34 | 787.361,87 |
| 2025-03 | residencial | 613.896,21 | 1.668 | 478.839,04 |

## Compactação da silver

Arquivos: 96 antes e 90 depois. Consulta de um medidor: 0.29s antes e 0.20s depois.

## Verificações

| verificacao | severidade | resultado | detalhe |
|---|---|---|---|
| gabarito: leituras únicas | critica | ok | 17235095 x 17235095 |
| silver sem chave repetida | critica | ok | 17235095 chaves |
| bronze guarda tudo o que chegou | critica | ok | 17751765 linhas |
| gabarito: linhas malformadas | critica | ok | 361 x 361 |
| gabarito: picos na quarentena | critica | ok | 2634 x 2634 |
| gabarito: energia por classe | critica | ok | maior erro relativo: 2.13e-08 |
| gabarito: trocas de medidor | critica | ok | 10 x 10 |
| nenhum retrocesso sem explicação | critica | ok | 0 intervalos |
| gabarito: interrupções encontradas | critica | ok | 100.0% de 79 pares de alimentador e dia |
| fraudes: recall | alerta | ok | 100.0% |
| fraudes: precisão | alerta | ok | 100.0% |
