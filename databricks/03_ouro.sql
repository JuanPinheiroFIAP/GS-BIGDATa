-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03. Ouro: fatos e dimensões para o MVP do Inclui+ (com LGPD)
-- MAGIC
-- MAGIC | Tabela | Papel |
-- MAGIC | --- | --- |
-- MAGIC | `ouro_fato_relato` | **fato**: um relato de acessibilidade (nota, tipo, fotos) |
-- MAGIC | `ouro_fato_clima_dia` | **fato**: clima do dia em cada lugar |
-- MAGIC | `ouro_dim_lugar` | dimensão: lugar do catálogo |
-- MAGIC | `ouro_dim_necessidade` | dimensão: visão, audição, mobilidade e cognitiva, com a população do IBGE |
-- MAGIC | `ouro_dim_tempo` | dimensão: calendário |
-- MAGIC | `ouro_dim_usuario` | dimensão: pessoa autora, **só com hash e e-mail mascarado** |
-- MAGIC | `ouro_osm_cobertura` | agregado: lugares do OSM por categoria e situação de acessibilidade |
-- MAGIC | **`ouro_mvp_nota_lugar`** | **a tabela que o MVP consome**: nota por lugar e necessidade, barreiras e o que falta |
-- MAGIC
-- MAGIC **LGPD na Ouro:** `autor_nome` não entra; `autor_email` vira `usuario_hash` (SHA-256 com sal) e `email_mascarado`.
-- MAGIC O sal vem do *widget* `salt_lgpd` (menu do notebook: Edit > Add widget) e **nunca** vai para o repositório.
-- MAGIC Use o mesmo sal sempre, senão o hash da mesma pessoa muda entre execuções.

-- COMMAND ----------

CREATE WIDGET TEXT salt_lgpd DEFAULT '';

-- COMMAND ----------

SELECT assert_true(length(:salt_lgpd) >= 8, 'Preencha o widget salt_lgpd com pelo menos 8 caracteres.') AS sal_ok;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW usuario_pseudonimo AS
SELECT
  usuario_id,
  sha2(concat_ws('|', :salt_lgpd, lower(max(autor_email))), 256) AS usuario_hash,
  concat(substr(max(autor_email), 1, 1), '***@', split(max(autor_email), '@')[1]) AS email_mascarado
FROM workspace.gs.prata_relatos
GROUP BY usuario_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_usuario AS
SELECT usuario_hash, email_mascarado FROM usuario_pseudonimo;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_lugar AS
SELECT lugar_id, nome, tipo, categoria, bairro, latitude, longitude, recursos,
       size(recursos) AS qtd_recursos, fotos_verificadas, degraus_entrada, largura_porta_cm
FROM workspace.gs.prata_lugares;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_necessidade AS
SELECT n.necessidade, n.nome_pt, n.descricao,
       i.pessoas AS pessoas_com_dificuldade,
       t.pessoas AS populacao_2_anos_ou_mais,
       round(100.0 * i.pessoas / t.pessoas, 2) AS pct_da_populacao
FROM (VALUES
  ('vision',    'Visão',     'Dificuldade permanente para enxergar'),
  ('hearing',   'Audição',   'Dificuldade permanente para ouvir'),
  ('mobility',  'Mobilidade','Dificuldade permanente para andar ou subir degraus'),
  ('cognitive', 'Cognitiva', 'Limitação nas funções mentais')
) AS n(necessidade, nome_pt, descricao)
LEFT JOIN workspace.gs.prata_ibge_pcd i
  ON i.necessidade = n.necessidade AND i.variavel_id = '12785'
CROSS JOIN (
  SELECT max(pessoas) AS pessoas FROM workspace.gs.prata_ibge_pcd
  WHERE variavel_id = '11852' AND necessidade = 'total'
) t;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_tempo AS
SELECT
  CAST(date_format(data, 'yyyyMMdd') AS INT) AS data_sk,
  data,
  year(data) AS ano,
  month(data) AS mes,
  day(data) AS dia,
  element_at(array('domingo','segunda','terça','quarta','quinta','sexta','sábado'), dayofweek(data)) AS dia_semana,
  dayofweek(data) IN (1, 7) AS fim_de_semana
FROM (
  SELECT explode(sequence(min(d), max(d), INTERVAL 1 DAY)) AS data
  FROM (
    SELECT data_relato AS d FROM workspace.gs.prata_relatos
    UNION ALL SELECT data FROM workspace.gs.prata_clima
  )
);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_fato_relato AS
SELECT
  r.relato_id,
  CAST(date_format(r.data_relato, 'yyyyMMdd') AS INT) AS data_sk,
  r.lugar_id,
  r.necessidade,
  u.usuario_hash,
  r.tipo,
  r.nota,
  r.fotos,
  r.criado_em,
  r.texto
FROM workspace.gs.prata_relatos r
JOIN usuario_pseudonimo u ON u.usuario_id = r.usuario_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_fato_clima_dia AS
SELECT
  lugar_id,
  CAST(date_format(data, 'yyyyMMdd') AS INT) AS data_sk,
  round(avg(temperatura_c), 1) AS temperatura_media_c,
  max(temperatura_c) AS temperatura_max_c,
  min(temperatura_c) AS temperatura_min_c,
  round(sum(chuva_mm), 1) AS chuva_total_mm,
  count_if(chuva_mm >= 0.1) AS horas_com_chuva
FROM workspace.gs.prata_clima
GROUP BY lugar_id, data;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_osm_cobertura AS
SELECT categoria, cadeira_rodas, count(*) AS qtd_lugares
FROM workspace.gs.prata_osm
GROUP BY categoria, cadeira_rodas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela que o MVP consome: `ouro_mvp_nota_lugar`
-- MAGIC
-- MAGIC Uma linha por lugar e necessidade. É o que o cartão do lugar mostra no Inclui+:
-- MAGIC a **nota por necessidade**, quantos relatos e barreiras existem e a lista **"o que falta"**
-- MAGIC (recursos importantes para a necessidade que o lugar não tem). Lugar sem relato aparece com nota nula.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_mvp_nota_lugar AS
SELECT
  l.lugar_id,
  l.nome,
  l.categoria,
  l.bairro,
  l.latitude,
  l.longitude,
  n.necessidade,
  n.nome_pt AS necessidade_pt,
  round(avg(f.nota), 1) AS nota_media,
  count(f.relato_id) AS qtd_relatos,
  count_if(f.tipo = 'barreira') AS qtd_barreiras,
  round(100.0 * count_if(f.tipo = 'barreira') / nullif(count(f.relato_id), 0), 1) AS pct_barreiras,
  sum(f.fotos) AS fotos_enviadas,
  max(f.criado_em) AS ultimo_relato_em,
  array_except(
    CASE n.necessidade
      WHEN 'mobility'  THEN array('ramp', 'elevator', 'restroom', 'parking')
      WHEN 'vision'    THEN array('braille', 'tactileFloor', 'audioGuide', 'guideDog')
      WHEN 'hearing'   THEN array('libras', 'captions', 'visualAlerts')
      WHEN 'cognitive' THEN array('plainLanguage', 'quietSpace', 'clearSignage')
    END,
    l.recursos) AS recursos_faltando
FROM workspace.gs.ouro_dim_lugar l
CROSS JOIN workspace.gs.ouro_dim_necessidade n
LEFT JOIN workspace.gs.ouro_fato_relato f
  ON f.lugar_id = l.lugar_id AND f.necessidade = n.necessidade
GROUP BY l.lugar_id, l.nome, l.categoria, l.bairro, l.latitude, l.longitude, l.recursos, n.necessidade, n.nome_pt;

-- COMMAND ----------

SELECT * FROM workspace.gs.ouro_mvp_nota_lugar ORDER BY nome, necessidade LIMIT 20;
