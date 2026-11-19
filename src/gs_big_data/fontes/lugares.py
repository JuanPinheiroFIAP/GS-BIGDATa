"""Fonte interna: catálogo de lugares do MVP Inclui+ (20 locais fictícios em São Paulo).

O MVP não tem backend; o catálogo é o arquivo site/assets/js/data/places.js do repositório do
produto, um objeto JavaScript (chaves sem aspas, aspas simples). Lemos o arquivo publicado no
GitHub e gravamos um registro JSON por lugar, com a estrutura original (inclusive os campos aninhados).
"""
from __future__ import annotations

import re
from pathlib import Path

import json5

from gs_big_data.arquivos import caminho_coleta, escrever_jsonl
from gs_big_data.web import sessao

FONTE = "lugares"
URL_PADRAO = (
    "https://raw.githubusercontent.com/kaikyalvaro1708/inclui-gs/main/site/assets/js/data/places.js"
)
_ARRAY_PLACES = re.compile(r"export\s+const\s+PLACES\s*=\s*(\[.*?\n\])\s*;", re.DOTALL)


def extrair_lugares(codigo_js: str) -> list[dict]:
    achou = _ARRAY_PLACES.search(codigo_js)
    if not achou:
        raise ValueError("Não encontrei 'export const PLACES = [...]' no arquivo baixado.")
    lugares = json5.loads(achou.group(1))
    if not lugares or not all({"id", "lat", "lng"} <= set(lugar) for lugar in lugares):
        raise ValueError("PLACES vazio ou com lugares sem id/lat/lng.")
    return lugares


def baixar_lugares(url: str = URL_PADRAO) -> list[dict]:
    resposta = sessao().get(url, timeout=30)
    resposta.raise_for_status()
    resposta.encoding = "utf-8"
    return extrair_lugares(resposta.text)


def coletar(ds: str, url: str = URL_PADRAO, base: Path | None = None) -> dict:
    lugares = baixar_lugares(url)
    caminho = caminho_coleta(FONTE, ds, base)
    return {"fonte": FONTE, "arquivo": caminho.name, "registros": escrever_jsonl(caminho, lugares)}
