-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05. Evidências para o relatório
-- MAGIC
-- MAGIC Consultas curtas (uma tela cada) que mostram o resultado da Prata e da Ouro depois dos dois lotes.
-- MAGIC Não altera nada: só lê as tabelas. Não precisa do widget `salt_lgpd`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prata: linhas por tabela (iguais depois do lote 2 e de rodar de novo: o MERGE não duplica)

-- COMMAND ----------

SELECT 'prata_lugares' AS tabela, count(*) AS linhas FROM workspace.gs.prata_lugares
UNION ALL SELECT 'prata_relatos', count(*) FROM workspace.gs.prata_relatos
UNION ALL SELECT 'prata_relatos_quarentena', count(*) FROM workspace.gs.prata_relatos_quarentena
UNION ALL SELECT 'prata_osm', count(*) FROM workspace.gs.prata_osm
UNION ALL SELECT 'prata_clima', count(*) FROM workspace.gs.prata_clima
UNION ALL SELECT 'prata_ibge_pcd', count(*) FROM workspace.gs.prata_ibge_pcd;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prata: regras de qualidade dos relatos (contagem do que cada regra barrou, tratou ou removeu)

-- COMMAND ----------

SELECT regra, acao, registros
FROM (
  SELECT *, row_number() OVER (PARTITION BY tabela, regra ORDER BY execucao DESC) AS ordem
  FROM workspace.gs.prata_qualidade
  WHERE tabela = 'prata_relatos'
)
WHERE ordem = 1
ORDER BY acao, regra;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prata: relatos em quarentena, por motivo

-- COMMAND ----------

SELECT motivos, count(*) AS relatos
FROM workspace.gs.prata_relatos_quarentena
GROUP BY motivos
ORDER BY relatos DESC, motivos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ouro: linhas por tabela (1 fato de relato, 1 fato de clima, 4 dimensões, 1 agregado e a tabela do MVP)

-- COMMAND ----------

SELECT 'ouro_fato_relato' AS tabela, count(*) AS linhas FROM workspace.gs.ouro_fato_relato
UNION ALL SELECT 'ouro_fato_clima_dia', count(*) FROM workspace.gs.ouro_fato_clima_dia
UNION ALL SELECT 'ouro_dim_lugar', count(*) FROM workspace.gs.ouro_dim_lugar
UNION ALL SELECT 'ouro_dim_necessidade', count(*) FROM workspace.gs.ouro_dim_necessidade
UNION ALL SELECT 'ouro_dim_tempo', count(*) FROM workspace.gs.ouro_dim_tempo
UNION ALL SELECT 'ouro_dim_usuario', count(*) FROM workspace.gs.ouro_dim_usuario
UNION ALL SELECT 'ouro_osm_cobertura', count(*) FROM workspace.gs.ouro_osm_cobertura
UNION ALL SELECT 'ouro_mvp_nota_lugar', count(*) FROM workspace.gs.ouro_mvp_nota_lugar;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## LGPD na Ouro: a dimensão de usuário só tem hash (SHA-256 com sal) e e-mail mascarado; nome e e-mail não existem aqui

-- COMMAND ----------

SELECT usuario_hash, email_mascarado
FROM workspace.gs.ouro_dim_usuario
ORDER BY usuario_hash
LIMIT 6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ouro: fato de relato ligado às dimensões (sem nome nem e-mail do autor)

-- COMMAND ----------

SELECT relato_id, data_sk, lugar_id, necessidade, left(usuario_hash, 12) AS usuario_hash_inicio, tipo, nota, fotos
FROM workspace.gs.ouro_fato_relato
ORDER BY data_sk, relato_id
LIMIT 6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MVP: a tabela que o cartão do lugar no Inclui+ consome (`ouro_mvp_nota_lugar`)

-- COMMAND ----------

SELECT necessidade_pt AS necessidade, nome, bairro, nota_media, qtd_relatos, qtd_barreiras, pct_barreiras
FROM workspace.gs.ouro_mvp_nota_lugar
WHERE qtd_relatos >= 3
ORDER BY nota_media DESC, qtd_relatos DESC, nome
LIMIT 8;
