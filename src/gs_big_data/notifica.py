"""Notificação por webhook (Discord ou Slack). A URL é segredo: vem de Variable, nunca do código."""
from __future__ import annotations

import logging

import requests

log = logging.getLogger(__name__)


def enviar_webhook(url: str | None, texto: str) -> bool:
    """Envia o texto ao webhook. Nunca levanta erro: avisar não pode derrubar a coleta."""
    if not url:
        return False
    try:
        # "content" é o campo do Discord; "text" é o do Slack. Cada um ignora o outro.
        resposta = requests.post(url, json={"content": texto, "text": texto}, timeout=10)
        resposta.raise_for_status()
        return True
    except Exception as erro:  # noqa: BLE001 - qualquer falha aqui só vira aviso no log
        # Não registrar a URL: ela carrega o token do webhook.
        log.warning("Não consegui enviar a notificação (%s).", type(erro).__name__)
        return False
