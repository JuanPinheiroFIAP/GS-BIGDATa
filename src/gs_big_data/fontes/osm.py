"""Fonte externa real: acessibilidade de lugares no OpenStreetMap (Overpass API, sem chave, ODbL).

Busca, no município de São Paulo, os estabelecimentos das mesmas categorias do Inclui+ que tenham a
tag "wheelchair" (yes, limited, no, designated). Cada elemento OSM vira uma linha, sem alteração.
Serve de universo de lugares da cidade e de referência independente sobre acessibilidade.
O servidor público às vezes responde 502/504 ou resposta parcial; por isso tentamos vários espelhos.
"""
from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path

from gs_big_data.arquivos import caminho_coleta, escrever_jsonl, ler_jsonl
from gs_big_data.web import USER_AGENT, sessao

log = logging.getLogger(__name__)

FONTE = "osm_acessibilidade"
ENDPOINTS_PADRAO = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
# Relação OSM 298285 (município de São Paulo) + 3600000000 = id da área no Overpass.
AREA_SAO_PAULO = 3600298285

CONSULTA = """[out:json][timeout:120];
area(id:{area})->.alvo;
(
  nwr["wheelchair"]["amenity"~"^(restaurant|cafe|cinema|theatre|clinic|doctors|hospital|pharmacy|school|library|marketplace)$"](area.alvo);
  nwr["wheelchair"]["tourism"~"^(museum|gallery)$"](area.alvo);
  nwr["wheelchair"]["shop"~"^(supermarket|books|mall)$"](area.alvo);
  nwr["wheelchair"]["leisure"~"^(park|fitness_centre)$"](area.alvo);
);
out center tags;"""


def consultar_overpass(consulta: str, endpoints: list[str], tentativas: int = 2) -> dict:
    s = sessao()
    erros: list[str] = []
    for url in endpoints:
        for tentativa in range(1, tentativas + 1):
            try:
                # O servidor principal responde em cerca de 1 min; os espelhos podem travar.
                resposta = s.post(url, data={"data": consulta}, timeout=150, headers={"User-Agent": USER_AGENT})
                if resposta.status_code in (429, 502, 503, 504):
                    raise RuntimeError(f"HTTP {resposta.status_code}")
                resposta.raise_for_status()
                dados = resposta.json()
                aviso = str(dados.get("remark", ""))
                if "runtime error" in aviso.lower():  # o Overpass responde 200 mesmo quando estoura o tempo
                    raise RuntimeError(aviso)
                if not dados.get("elements"):
                    raise RuntimeError("resposta sem elementos")
                return dados
            except Exception as erro:  # noqa: BLE001 - tenta o próximo espelho
                erros.append(f"{url} (tentativa {tentativa}): {erro}")
                log.warning("Overpass falhou: %s", erros[-1])
                time.sleep(min(30, 5 * tentativa))
    raise RuntimeError("Overpass indisponível. " + " | ".join(erros))


def _coleta_recente(destino: Path, max_idade_horas: float) -> Path | None:
    """Arquivo OSM de outro dia coletado há pouco tempo, se houver."""
    if max_idade_horas <= 0 or not destino.parent.exists():
        return None
    limite = time.time() - max_idade_horas * 3600
    candidatos = [
        p for p in destino.parent.glob(f"{FONTE}_*.jsonl") if p.name != destino.name and p.stat().st_mtime >= limite
    ]
    return max(candidatos, key=lambda p: p.stat().st_mtime, default=None)


def coletar(
    ds: str,
    endpoints: list[str] | None = None,
    area: int = AREA_SAO_PAULO,
    base: Path | None = None,
    max_idade_horas: float = 0,
) -> dict:
    """Consulta o Overpass e grava o arquivo do dia.

    O OSM é um retrato do momento: ao recuperar vários dias de uma vez (catchup), consultar de novo a cada
    dia não traria dado novo e sobrecarregaria o servidor público. Com max_idade_horas > 0, um arquivo de
    outro dia coletado dentro dessa janela é copiado para o nome do dia em vez de repetir a consulta.
    """
    caminho = caminho_coleta(FONTE, ds, base)
    recente = _coleta_recente(caminho, max_idade_horas)
    if recente is not None:
        log.info("Reaproveitando %s (coletado há menos de %sh) em vez de consultar o Overpass.", recente.name, max_idade_horas)
        temporario = caminho.with_name(caminho.name + ".tmp")
        shutil.copyfile(recente, temporario)
        os.replace(temporario, caminho)
        registros = sum(1 for _ in ler_jsonl(caminho))
        return {"fonte": FONTE, "arquivo": caminho.name, "registros": registros, "reaproveitado_de": recente.name}
    dados = consultar_overpass(CONSULTA.format(area=area), endpoints or ENDPOINTS_PADRAO)
    return {"fonte": FONTE, "arquivo": caminho.name, "registros": escrever_jsonl(caminho, dados["elements"])}
