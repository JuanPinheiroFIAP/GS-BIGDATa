"""Pasta de pouso (chegada/): um arquivo .jsonl por fonte e por coleta, com a data no nome.

Layout:  chegada/<fonte>/<fonte>_<AAAA-MM-DD>.jsonl   (uma linha = um registro JSON)
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable, Iterator


def pasta_chegada() -> Path:
    return Path(os.environ.get("GS_CHEGADA_DIR", "/opt/airflow/chegada"))


def caminho_coleta(fonte: str, ds: str, base: Path | None = None) -> Path:
    return (base or pasta_chegada()) / fonte / f"{fonte}_{ds}.jsonl"


def escrever_jsonl(destino: Path, registros: Iterable[dict]) -> int:
    """Grava os registros de forma atômica (arquivo temporário + rename).

    Reexecutar a coleta do mesmo dia sobrescreve o mesmo arquivo, sem deixar
    arquivo pela metade para quem estiver lendo a pasta.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    total = 0
    with temporario.open("w", encoding="utf-8", newline="\n") as arquivo:
        for registro in registros:
            arquivo.write(json.dumps(registro, ensure_ascii=False, separators=(",", ":")) + "\n")
            total += 1
    os.replace(temporario, destino)
    return total


def ler_jsonl(caminho: Path) -> Iterator[dict]:
    with caminho.open(encoding="utf-8") as arquivo:
        for numero, linha in enumerate(arquivo, start=1):
            if not linha.strip():
                continue
            try:
                yield json.loads(linha)
            except json.JSONDecodeError as erro:
                raise ValueError(f"{caminho.name}, linha {numero}: JSON inválido ({erro})") from erro


def conferir(ds: str, fontes: Iterable[str], base: Path | None = None) -> dict[str, dict]:
    """Confere se o arquivo de cada fonte chegou, não está vazio e é JSONL válido."""
    resumo: dict[str, dict] = {}
    faltando: list[str] = []
    for fonte in fontes:
        caminho = caminho_coleta(fonte, ds, base)
        if not caminho.exists() or caminho.stat().st_size == 0:
            faltando.append(fonte)
            continue
        registros = sum(1 for _ in ler_jsonl(caminho))
        if registros == 0:
            faltando.append(fonte)
            continue
        resumo[fonte] = {"arquivo": caminho.name, "bytes": caminho.stat().st_size, "registros": registros}
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios para {ds}: {', '.join(faltando)}")
    return resumo
