"""Fonte externa real: pessoas com deficiência em São Paulo, Censo 2022 (API de agregados do IBGE).

Tabela 10127: pessoas de 2 anos ou mais com deficiência, por tipo de dificuldade funcional
(enxergar, ouvir, andar, pegar objetos, funções mentais). Os tipos correspondem às necessidades do
Inclui+ (visão, audição, mobilidade, cognitivo) e dão a demanda para medir a cobertura do produto.
Cada elemento da lista devolvida pela API (uma variável) vira uma linha, sem alteração.
"""
from __future__ import annotations

from pathlib import Path

from gs_big_data.arquivos import caminho_coleta, escrever_jsonl
from gs_big_data.web import sessao

FONTE = "ibge_pcd"
URL = "https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos/{periodo}/variaveis/{variaveis}"
# 12785 = pessoas com deficiência; 11852 = total de pessoas de 2 anos ou mais
VARIAVEIS = "12785|11852"


def coletar(
    ds: str,
    municipio: str = "3550308",  # São Paulo (SP)
    tabela: str = "10127",
    periodo: str = "2022",
    base: Path | None = None,
) -> dict:
    resposta = sessao().get(
        URL.format(tabela=tabela, periodo=periodo, variaveis=VARIAVEIS),
        params={"localidades": f"N6[{municipio}]", "classificacao": "1509[all]"},
        timeout=60,
    )
    resposta.raise_for_status()
    dados = resposta.json()
    if not isinstance(dados, list) or not dados:
        raise ValueError("A API do IBGE devolveu uma resposta vazia.")
    caminho = caminho_coleta(FONTE, ds, base)
    return {"fonte": FONTE, "arquivo": caminho.name, "registros": escrever_jsonl(caminho, dados)}
