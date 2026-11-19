"""Fonte simulada: relatos de acessibilidade enviados por pessoas usuárias do Inclui+.

Por que simular: o MVP não tem backend (os relatos ficam no navegador de cada pessoa) e dados reais
de pessoas com deficiência são dados pessoais sensíveis (LGPD, art. 5º, II). Geramos o fluxo que um
backend produziria, com o mesmo formato dos relatos do MVP (necessidade, texto, lugar), mais nota,
fotos e autor (nome e e-mail sintéticos, domínio example.com reservado pela IANA, para exercitar a LGPD).

Regras da geração (tudo documentado, nada escondido):
* Determinística por data: a mesma data sempre gera o mesmo arquivo. Reexecutar não muda nada;
  datas diferentes geram relatos novos (lotes incrementais).
* A necessidade (visão, mobilidade, audição, cognitiva) segue as proporções do Censo 2022 do IBGE
  para São Paulo (tabela 10127), as mesmas da fonte ibge_pcd.
* O tom do relato acompanha os recursos do lugar no catálogo: quanto mais recursos para a necessidade,
  maior a chance de elogio. Por isso conclusões tiradas destes dados são ilustrativas, não evidência.
* Uma pequena parcela dos registros nasce suja de propósito (nota inválida, lugar inexistente,
  e-mail quebrado, duplicata...) para exercitar as regras de qualidade da camada Prata.
  A contagem exata de cada defeito é devolvida pela tarefa (o "gabarito"), nunca gravada no arquivo.
"""
from __future__ import annotations

import random
import unicodedata
from collections import Counter
from pathlib import Path

from gs_big_data.arquivos import caminho_coleta, escrever_jsonl

FONTE = "relatos"
TAXA_SUJEIRA_PADRAO = 0.06

# Pessoas com dificuldade permanente, Censo 2022, São Paulo (SIDRA 10127).
NECESSIDADES = {"vision": 400_673, "mobility": 267_895, "hearing": 145_914, "cognitive": 140_256}

# Recursos do catálogo (campo "features" do places.js) que importam para cada necessidade.
RECURSOS = {
    "mobility": ("ramp", "elevator", "restroom", "parking"),
    "vision": ("braille", "tactileFloor", "audioGuide", "guideDog"),
    "hearing": ("libras", "captions", "visualAlerts"),
    "cognitive": ("plainLanguage", "quietSpace", "clearSignage"),
}

TEXTOS = {
    ("mobility", "elogio"): [
        "Entrada sem degraus e porta larga, consegui entrar de cadeira de rodas sem ajuda.",
        "Rampa bem sinalizada e banheiro adaptado com espaço de sobra para manobrar.",
        "Elevador funcionando e equipe atenciosa, circulei por todo o espaço com autonomia.",
    ],
    ("mobility", "barreira"): [
        "Degrau na entrada e nenhuma rampa, precisei de ajuda para subir.",
        "Porta estreita e corredor apertado, a cadeira de rodas não passou.",
        "O banheiro adaptado estava sendo usado como depósito e não consegui usar.",
    ],
    ("vision", "elogio"): [
        "Cardápio em braile e equipe disposta a descrever as opções com calma.",
        "Piso tátil até a recepção e cão-guia bem recebido.",
        "Sinalização com letras grandes e bom contraste, consegui me localizar sem ajuda.",
    ],
    ("vision", "barreira"): [
        "Nenhuma informação em braile ou em áudio, dependi de outras pessoas para ler o cardápio.",
        "Degraus mal sinalizados e pouca iluminação, quase tropecei na entrada.",
        "A entrada do cão-guia foi questionada pela equipe e precisei explicar a lei.",
    ],
    ("hearing", "elogio"): [
        "Atendimento em Libras e legendas nos vídeos, entendi tudo sem escrever no celular.",
        "Avisos visuais na chamada de senha, não perdi minha vez.",
        "A equipe falou de frente e com calma, o que facilitou a leitura labial.",
    ],
    ("hearing", "barreira"): [
        "Chamada de senha só por voz, perdi minha vez e ninguém avisou.",
        "Sem intérprete de Libras nem legendas na sessão, não acompanhei o conteúdo.",
        "Ambiente muito barulhento e sem alternativas visuais, não consegui me comunicar.",
    ],
    ("cognitive", "elogio"): [
        "Linguagem simples nos cartazes e um espaço calmo para descansar, atendimento acolhedor.",
        "Placas claras e equipe paciente, entendi cada etapa sem pressa.",
        "Horário tranquilo e pouca gente, consegui resolver tudo com calma.",
    ],
    ("cognitive", "barreira"): [
        "Muito barulho e luz forte, não consegui permanecer no local.",
        "Instruções confusas e com palavras difíceis, precisei de ajuda para entender.",
        "Fila longa e sem sinalização clara, o atendimento foi apressado.",
    ],
}

_NOMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela", "João",
    "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Paulo", "Rafaela", "Sérgio", "Tatiane", "Vinícius",
    "Beatriz", "Caio", "Débora", "Eduardo", "Fernanda", "Gustavo", "Helena", "Igor", "Juliana", "Leonardo",
    "Marcos", "Natália", "Otávio", "Patrícia", "Ricardo", "Sabrina", "Thiago", "Valéria", "Wesley", "Yasmin",
]
_SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Lima", "Carvalho", "Ribeiro", "Almeida", "Gomes",
    "Martins", "Araújo", "Barbosa", "Rocha", "Dias", "Nascimento", "Moreira", "Cardoso", "Teixeira", "Correia",
]

DEFEITOS = (
    "nota_fora_da_faixa",
    "nota_nao_numerica",
    "lugar_inexistente",
    "necessidade_invalida",
    "email_invalido",
    "sem_data_hora",
    "duplicado",
)


def _sem_acento(texto: str) -> str:
    return unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode().lower()


def _gerar_usuarios(quantidade: int = 80) -> list[dict]:
    """Pessoas sintéticas fixas (mesma semente sempre), para o mesmo autor aparecer em vários dias."""
    rng = random.Random("gs-usuarios")
    usuarios = []
    for i in range(1, quantidade + 1):
        nome, sobrenome = rng.choice(_NOMES), rng.choice(_SOBRENOMES)
        usuarios.append(
            {
                "usuario_id": f"u{i:03d}",
                "autor_nome": f"{nome} {sobrenome}",
                "autor_email": f"{_sem_acento(nome)}.{_sem_acento(sobrenome)}{i}@example.com",
            }
        )
    return usuarios


USUARIOS = _gerar_usuarios()


def _cobertura(lugar: dict, necessidade: str) -> float:
    recursos = RECURSOS[necessidade]
    return len(set(lugar.get("features", [])) & set(recursos)) / len(recursos)


def gerar(
    ds: str,
    lugares: list[dict],
    quantidade: int | None = None,
    taxa_sujeira: float = TAXA_SUJEIRA_PADRAO,
) -> tuple[list[dict], dict[str, int]]:
    """Devolve (registros do dia, gabarito com a contagem de cada defeito injetado)."""
    rng = random.Random(f"gs-relatos|{ds}")
    total = quantidade if quantidade is not None else rng.randint(25, 45)
    necessidades = list(NECESSIDADES)
    pesos = list(NECESSIDADES.values())

    registros: list[dict] = []
    for i in range(1, total + 1):
        lugar = rng.choice(lugares)
        necessidade = rng.choices(necessidades, weights=pesos)[0]
        usuario = rng.choice(USUARIOS)
        elogio = rng.random() < 0.25 + 0.6 * _cobertura(lugar, necessidade)
        tipo = "elogio" if elogio else "barreira"
        nota = rng.choice([4, 5, 5]) if elogio else rng.choice([1, 2, 3, 3])
        hora, minuto, segundo = rng.randint(6, 23), rng.randint(0, 59), rng.randint(0, 59)
        registros.append(
            {
                "relato_id": f"{ds}-{i:03d}",
                "criado_em": f"{ds}T{hora:02d}:{minuto:02d}:{segundo:02d}-03:00",
                **usuario,
                "lugar_id": lugar["id"],
                "necessidade": necessidade,
                "tipo": tipo,
                "nota": nota,
                "fotos": rng.choice([0, 0, 1, 1, 2, 3]),
                "texto": rng.choice(TEXTOS[(necessidade, tipo)]),
            }
        )

    gabarito = _injetar_sujeira(registros, ds, taxa_sujeira)
    return registros, gabarito


def _injetar_sujeira(registros: list[dict], ds: str, taxa: float) -> dict[str, int]:
    rng = random.Random(f"gs-sujeira|{ds}")
    contagem: Counter[str] = Counter()
    duplicatas: list[dict] = []
    for registro in registros:
        if rng.random() >= taxa:
            continue
        defeito = rng.choice(DEFEITOS)
        contagem[defeito] += 1
        if defeito == "nota_fora_da_faixa":
            registro["nota"] = rng.choice([0, 6, 9, -1])
        elif defeito == "nota_nao_numerica":
            registro["nota"] = rng.choice(["N/A", "cinco", ""])
        elif defeito == "lugar_inexistente":
            registro["lugar_id"] = f"lugar-{rng.randint(1000, 9999)}"
        elif defeito == "necessidade_invalida":
            registro["necessidade"] = rng.choice([None, "", "outro", "MOBILITY ", " Vision"])
        elif defeito == "email_invalido":
            registro["autor_email"] = registro["autor_email"].replace("@", "")
        elif defeito == "sem_data_hora":
            registro["criado_em"] = None
        elif defeito == "duplicado":
            duplicatas.append(dict(registro))
    registros.extend(duplicatas)
    return dict(contagem)


def coletar(
    ds: str,
    lugares: list[dict],
    taxa_sujeira: float = TAXA_SUJEIRA_PADRAO,
    base: Path | None = None,
) -> dict:
    registros, gabarito = gerar(ds, lugares, taxa_sujeira=taxa_sujeira)
    caminho = caminho_coleta(FONTE, ds, base)
    return {
        "fonte": FONTE,
        "arquivo": caminho.name,
        "registros": escrever_jsonl(caminho, registros),
        "defeitos_injetados": gabarito,
    }
