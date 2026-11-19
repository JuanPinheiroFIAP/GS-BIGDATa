"""Sessão HTTP compartilhada pelas coletas: User-Agent identificado e repetição em falhas passageiras."""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Identifica o projeto para quem mantém as APIs, sem dados pessoais.
USER_AGENT = "gs-big-data-fiap/0.1 (projeto academico; coleta de dados abertos)"


def sessao() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = USER_AGENT
    return s
