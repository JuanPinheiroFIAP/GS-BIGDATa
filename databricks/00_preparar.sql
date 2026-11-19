-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00. Preparar o ambiente (Databricks Free Edition)
-- MAGIC
-- MAGIC Cria o esquema `workspace.gs`, o Volume `chegada` (pasta de pouso, onde sobem os arquivos do Airflow)
-- MAGIC e a tabela que registra as regras de qualidade da Prata. Pode rodar mais de uma vez.
-- MAGIC
-- MAGIC Depois de rodar, suba os arquivos ao Volume pela interface (Catalog > workspace > gs > chegada),
-- MAGIC uma pasta por fonte: `chegada/lugares/`, `chegada/osm_acessibilidade/`, `chegada/ibge_pcd/`,
-- MAGIC `chegada/clima/` e `chegada/relatos/`. Use `python scripts/preparar_lotes.py` para separar os arquivos em lotes.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.gs;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.gs.chegada;

-- COMMAND ----------

-- Uma linha por regra e por execução da Prata. "acao" diz o que aconteceu com os registros:
-- barrado (vai para a quarentena ou é descartado), removido (duplicata) ou tratado (corrigido e mantido).
CREATE TABLE IF NOT EXISTS workspace.gs.prata_qualidade (
  execucao  TIMESTAMP,
  tabela    STRING,
  regra     STRING,
  acao      STRING,
  registros BIGINT
);
