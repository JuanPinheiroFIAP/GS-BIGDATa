#!/usr/bin/env python3
"""Cria o .env a partir do .env.example trocando os "troque-me" por segredos aleatórios.

Uso:  python scripts/gerar_env.py            (não sobrescreve um .env existente)
      python scripts/gerar_env.py --force    (sobrescreve)
Só usa a biblioteca padrão. Não imprime os segredos: eles ficam no .env.
"""
from __future__ import annotations

import base64
import os
import secrets
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
MODELO = RAIZ / ".env.example"
DESTINO = RAIZ / ".env"
PLACEHOLDER = "troque-me"


def novo_valor(chave: str) -> str:
    if chave == "FERNET_KEY":
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    if chave == "AIRFLOW_JWT_SECRET":
        return secrets.token_hex(32)
    # token_urlsafe só usa letras, números, "-" e "_": seguro dentro da URL do Postgres.
    return secrets.token_urlsafe(18)


def main() -> int:
    if DESTINO.exists() and "--force" not in sys.argv:
        print(f"{DESTINO.name} já existe; nada foi alterado (use --force para recriar).")
        return 0
    linhas = []
    for linha in MODELO.read_text(encoding="utf-8").splitlines():
        chave, separador, valor = linha.partition("=")
        if separador and not chave.startswith("#") and valor.strip() == PLACEHOLDER:
            linha = f"{chave}={novo_valor(chave.strip())}"
        linhas.append(linha)
    DESTINO.write_text("\n".join(linhas) + "\n", encoding="utf-8")
    print(f"{DESTINO.name} criado com segredos aleatórios.")
    print("Usuário e senha do Airflow: variáveis _AIRFLOW_WWW_USER_* dentro do .env.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
