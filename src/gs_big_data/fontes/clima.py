"""Fonte externa real: clima por lugar (Open-Meteo, sem chave).

Uma chamada com as coordenadas de todos os lugares do catálogo. A resposta da API traz uma lista
de objetos na mesma ordem das coordenadas; gravamos cada objeto como veio e só acrescentamos
"lugar_id", porque a Open-Meteo ajusta as coordenadas à sua grade e o cruzamento por lat/lng falharia.
Única remoção: "generationtime_ms" (ver coletar).
"""
from __future__ import annotations

from pathlib import Path

from gs_big_data.arquivos import caminho_coleta, escrever_jsonl
from gs_big_data.web import sessao

FONTE = "clima"
URL = "https://archive-api.open-meteo.com/v1/archive"


def coletar(ds: str, lugares: list[dict], base: Path | None = None) -> dict:
    resposta = sessao().get(
        URL,
        params={
            "latitude": ",".join(str(lugar["lat"]) for lugar in lugares),
            "longitude": ",".join(str(lugar["lng"]) for lugar in lugares),
            "start_date": ds,
            "end_date": ds,
            "hourly": "temperature_2m,precipitation",
            "timezone": "America/Sao_Paulo",
        },
        timeout=60,
    )
    resposta.raise_for_status()
    dados = resposta.json()
    if isinstance(dados, dict):  # com uma única coordenada a API devolve um objeto
        dados = [dados]
    if len(dados) != len(lugares):
        raise ValueError(f"Open-Meteo devolveu {len(dados)} locais para {len(lugares)} pedidos.")
    if not any(t is not None for item in dados for t in item["hourly"]["temperature_2m"]):
        # O arquivo histórico tem atraso de 1 a 2 dias: sem dados, melhor tentar de novo mais tarde.
        raise ValueError(f"Open-Meteo ainda sem dados para {ds}.")
    # "generationtime_ms" é o tempo de processamento do servidor e muda a cada chamada;
    # sem ele, reexecutar o dia gera exatamente o mesmo arquivo.
    registros = [
        {"lugar_id": lugar["id"], **{k: v for k, v in item.items() if k != "generationtime_ms"}}
        for lugar, item in zip(lugares, dados)
    ]
    caminho = caminho_coleta(FONTE, ds, base)
    return {"fonte": FONTE, "arquivo": caminho.name, "registros": escrever_jsonl(caminho, registros)}
