-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04. Cinco consultas SQL que respondem perguntas do MVP (Inclui+)
-- MAGIC
-- MAGIC Todas leem a Ouro. A 5ª é a de **impacto**.
-- MAGIC Atenção: os relatos são simulados; as interpretações no relatório devem dizer isso.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 1. Quais são os 3 lugares melhor avaliados para cada necessidade?
-- MAGIC *Pergunta do cidadão:* "Onde eu vou com mais segurança, dada a minha necessidade?" Exige pelo menos 2 relatos, para a nota não vir de um caso isolado.

-- COMMAND ----------

SELECT necessidade_pt AS necessidade, posicao, nome, bairro, nota_media, qtd_relatos
FROM (
  SELECT *, row_number() OVER (PARTITION BY necessidade ORDER BY nota_media DESC, qtd_relatos DESC, nome) AS posicao
  FROM workspace.gs.ouro_mvp_nota_lugar
  WHERE qtd_relatos >= 2
)
WHERE posicao <= 3
ORDER BY necessidade, posicao;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 2. O que falta nos lugares com mais barreiras de mobilidade?
-- MAGIC *Pergunta:* "Por que esse lugar é ruim para cadeira de rodas e o que ele poderia mudar?" É a lista "o que falta" do cartão do lugar.

-- COMMAND ----------

SELECT nome, bairro, qtd_relatos, qtd_barreiras, pct_barreiras, recursos_faltando
FROM workspace.gs.ouro_mvp_nota_lugar
WHERE necessidade = 'mobility' AND qtd_barreiras > 0
ORDER BY pct_barreiras DESC, qtd_barreiras DESC, nome
LIMIT 8;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 3. Dia de chuva piora a experiência de quem tem dificuldade de mobilidade?
-- MAGIC *Pergunta:* "Hoje chove; vale a pena sair?" Cruza o relato com o clima do mesmo lugar e dia.
-- MAGIC Chuva forte = 5 mm ou mais no dia (nos dias coletados, a mediana foi 4,9 mm; 1 mm separaria só 1 relato do resto).

-- COMMAND ----------

SELECT
  CASE WHEN c.chuva_total_mm >= 5 THEN 'chuva forte (5 mm ou mais)' ELSE 'chuva fraca ou sem chuva' END AS clima_do_dia,
  count(*) AS relatos,
  round(avg(f.nota), 2) AS nota_media,
  round(100.0 * count_if(f.tipo = 'barreira') / count(*), 1) AS pct_barreiras
FROM workspace.gs.ouro_fato_relato f
JOIN workspace.gs.ouro_fato_clima_dia c ON c.lugar_id = f.lugar_id AND c.data_sk = f.data_sk
WHERE f.necessidade = 'mobility'
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 4. Como o volume de relatos e a parcela de barreiras variam por dia?
-- MAGIC *Pergunta (equipe e parceiros):* "A comunidade está ativa? As barreiras estão diminuindo?"

-- COMMAND ----------

SELECT t.data, t.dia_semana, count(f.relato_id) AS relatos,
       round(avg(f.nota), 2) AS nota_media,
       round(100.0 * count_if(f.tipo = 'barreira') / nullif(count(f.relato_id), 0), 1) AS pct_barreiras
FROM workspace.gs.ouro_dim_tempo t
LEFT JOIN workspace.gs.ouro_fato_relato f ON f.data_sk = t.data_sk
GROUP BY t.data, t.dia_semana
HAVING count(f.relato_id) > 0
ORDER BY t.data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 5 (impacto). Quantas pessoas com dificuldade existem para cada lugar bem avaliado?
-- MAGIC *Pergunta:* "O Inclui+ já cobre a demanda real da cidade?"
-- MAGIC Demanda = Censo 2022 do IBGE (São Paulo). Oferta = lugares com nota média 4 ou mais e pelo menos 2 relatos.
-- MAGIC As colunas do OpenStreetMap mostram o tamanho do universo de lugares a mapear: quantos lugares da cidade têm
-- MAGIC `wheelchair` registrado e quantos o Inclui+ cobre hoje.

-- COMMAND ----------

WITH oferta AS (
  SELECT necessidade, count_if(nota_media >= 4 AND qtd_relatos >= 2) AS lugares_bem_avaliados
  FROM workspace.gs.ouro_mvp_nota_lugar
  GROUP BY necessidade
),
osm AS (
  SELECT sum(qtd_lugares) AS lugares_osm_mapeados,
         sum(CASE WHEN cadeira_rodas IN ('yes', 'designated') THEN qtd_lugares ELSE 0 END) AS lugares_osm_acessiveis
  FROM workspace.gs.ouro_osm_cobertura
),
inclui AS (SELECT count(*) AS lugares_inclui FROM workspace.gs.ouro_dim_lugar)
SELECT
  n.nome_pt AS necessidade,
  n.pessoas_com_dificuldade,
  o.lugares_bem_avaliados,
  round(n.pessoas_com_dificuldade / nullif(o.lugares_bem_avaliados, 0)) AS pessoas_por_lugar_bem_avaliado,
  osm.lugares_osm_acessiveis,
  inclui.lugares_inclui,
  round(100.0 * inclui.lugares_inclui / osm.lugares_osm_mapeados, 2) AS pct_do_universo_osm_coberto
FROM workspace.gs.ouro_dim_necessidade n
JOIN oferta o ON o.necessidade = n.necessidade
CROSS JOIN osm
CROSS JOIN inclui
ORDER BY n.pessoas_com_dificuldade DESC;
