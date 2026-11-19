"""DAG gs_coleta: coleta diária das fontes do Inclui+ na pasta de pouso chegada/.

Fluxo (as tarefas de uma mesma linha rodam em paralelo):

    inicio -> [coletar_lugares, coletar_osm, coletar_ibge]
    coletar_lugares -> [coletar_clima, gerar_relatos]
    [coletar_osm, coletar_ibge, coletar_clima, gerar_relatos] -> conferir_chegada -> notificar_resumo

Cada coleta grava chegada/<fonte>/<fonte>_<data>.jsonl, onde <data> é o dia coberto pela execução.
Reexecutar um dia sobrescreve o mesmo arquivo (idempotente).
Credenciais e URLs sensíveis vêm de Variables (AIRFLOW_VAR_* ou Admin > Variables).
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Variable, task

from gs_big_data import arquivos, notifica
from gs_big_data.fontes import clima, ibge, lugares, osm, relatos

log = logging.getLogger(__name__)

FUSO = "America/Sao_Paulo"
FONTES = [lugares.FONTE, osm.FONTE, ibge.FONTE, clima.FONTE, relatos.FONTE]


def _variable(nome: str, padrao: str = "") -> str:
    # Lida na hora da execução (não no parse do DAG). Vem de AIRFLOW_VAR_<NOME> ou do banco.
    return Variable.get(nome, default=padrao)


def _dia_da_execucao(contexto: dict) -> str:
    logical = contexto.get("logical_date") or getattr(contexto.get("dag_run"), "logical_date", None)
    return str(logical)[:10] if logical else str(contexto.get("run_id", "?"))


def avisar_falha(contexto: dict) -> None:
    """on_failure_callback de qualquer tarefa: registra no log e, se houver webhook, avisa."""
    try:
        ti = contexto["task_instance"]
        texto = f"[{ti.dag_id}] FALHA na tarefa {ti.task_id} (dia {_dia_da_execucao(contexto)}). Veja o log no Airflow."
        log.error(texto)
        notifica.enviar_webhook(_variable("WEBHOOK_NOTIFICACAO_SECRET"), texto)
    except Exception:  # noqa: BLE001 - o aviso nunca pode mascarar a falha original
        log.exception("Erro ao notificar a falha.")


with DAG(
    dag_id="gs_coleta",
    description="Coleta diária das fontes do Inclui+ na pasta chegada/",
    # Diário, à meia-noite de Brasília. Com intervalo de dados, a execução de data D cobre o dia D inteiro
    # e só dispara quando ele termina, então a data D (ds) é sempre um dia completo.
    schedule=CronDataIntervalTimetable("@daily", timezone=FUSO),
    start_date=pendulum.datetime(2026, 11, 12, tz=FUSO),
    catchup=True,
    max_active_runs=1,  # um dia por vez: gentil com as APIs públicas
    default_args={
        "owner": "gs-big-data",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": avisar_falha,
    },
    tags=["gs-big-data", "inclui", "coleta"],
    doc_md=__doc__,
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    @task(task_id="coletar_lugares")
    def coletar_lugares(ds=None):
        return lugares.coletar(ds, url=_variable("INCLUI_PLACES_URL", lugares.URL_PADRAO))

    @task(task_id="coletar_osm", execution_timeout=timedelta(minutes=20))
    def coletar_osm(ds=None):
        urls = [u.strip() for u in _variable("OVERPASS_URLS").split(",") if u.strip()]
        janela = float(_variable("OSM_REAPROVEITAR_HORAS", "12"))  # 0 = sempre consultar o Overpass
        return osm.coletar(ds, endpoints=urls or None, max_idade_horas=janela)

    @task(task_id="coletar_ibge")
    def coletar_ibge(ds=None):
        return ibge.coletar(ds)

    @task(task_id="coletar_clima")
    def coletar_clima(ds=None):
        # Depende de lugares: usa as coordenadas do catálogo coletado neste mesmo dia.
        catalogo = list(arquivos.ler_jsonl(arquivos.caminho_coleta(lugares.FONTE, ds)))
        return clima.coletar(ds, catalogo)

    @task(task_id="gerar_relatos")
    def gerar_relatos(ds=None):
        catalogo = list(arquivos.ler_jsonl(arquivos.caminho_coleta(lugares.FONTE, ds)))
        taxa = float(_variable("TAXA_SUJEIRA_RELATOS", str(relatos.TAXA_SUJEIRA_PADRAO)))
        resultado = relatos.coletar(ds, catalogo, taxa_sujeira=taxa)
        log.info("Defeitos injetados de propósito (gabarito da Prata): %s", resultado["defeitos_injetados"])
        return resultado

    @task(task_id="conferir_chegada")
    def conferir_chegada(ds=None):
        resumo = arquivos.conferir(ds, FONTES)  # falha se faltar arquivo, estiver vazio ou inválido
        for fonte, info in resumo.items():
            log.info("%s: %s registros, %s bytes (%s)", fonte, info["registros"], info["bytes"], info["arquivo"])
        return resumo

    @task(task_id="notificar_resumo")
    def notificar_resumo(resumo: dict, ds=None):
        partes = ", ".join(f"{fonte}={info['registros']}" for fonte, info in resumo.items())
        texto = f"[gs_coleta] {ds} concluída. Registros: {partes}."
        log.info(texto)
        enviado = notifica.enviar_webhook(_variable("WEBHOOK_NOTIFICACAO_SECRET"), texto)
        log.info("Webhook %s.", "enviado" if enviado else "não configurado ou indisponível (só log)")

    t_lugares = coletar_lugares()
    t_osm = coletar_osm()
    t_ibge = coletar_ibge()
    t_clima = coletar_clima()
    t_relatos = gerar_relatos()
    t_conferir = conferir_chegada()

    inicio >> [t_lugares, t_osm, t_ibge]
    t_lugares >> [t_clima, t_relatos]
    [t_osm, t_ibge, t_clima, t_relatos] >> t_conferir
    notificar_resumo(t_conferir)
