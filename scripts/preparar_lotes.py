#!/usr/bin/env python3
"""Separa os arquivos de chegada/ em lotes por faixa de datas, prontos para subir ao Volume do Databricks.

Uso:  python scripts/preparar_lotes.py                      (lote 1 = 12 a 14/11, lote 2 = 15 e 16/11)
      python scripts/preparar_lotes.py 1=2026-11-12:2026-11-13 2=2026-11-14:2026-11-16

Cria lotes/lote_<N>/<fonte>/<fonte>_<data>.jsonl (a pasta lotes/ não vai para o Git).
O Databricks Free Edition não se conecta ao Airflow: suba cada pasta de fonte pela interface
(Catalog > workspace > gs > chegada), um lote por vez.
"""
from __future__ import annotations

import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CHEGADA = RAIZ / "chegada"
LOTES = RAIZ / "lotes"
PADRAO = ["1=2026-11-12:2026-11-14", "2=2026-11-15:2026-11-16"]


def dias(inicio: str, fim: str):
    d, ultimo = date.fromisoformat(inicio), date.fromisoformat(fim)
    while d <= ultimo:
        yield d.isoformat()
        d += timedelta(days=1)


def main(argumentos: list[str]) -> int:
    for especificacao in argumentos or PADRAO:
        numero, _, faixa = especificacao.partition("=")
        inicio, _, fim = faixa.partition(":")
        destino = LOTES / f"lote_{numero}"
        copiados = 0
        for pasta in sorted(p for p in CHEGADA.iterdir() if p.is_dir() and not p.name.startswith("_")):
            for dia in dias(inicio, fim or inicio):
                origem = pasta / f"{pasta.name}_{dia}.jsonl"
                if origem.exists():
                    (destino / pasta.name).mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(origem, destino / pasta.name / origem.name)
                    copiados += 1
        print(f"lote_{numero}: {copiados} arquivos em {destino.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
