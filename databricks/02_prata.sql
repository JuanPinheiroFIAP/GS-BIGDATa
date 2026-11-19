-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02. Prata: tipos corretos, nulos e inválidos tratados, MERGE pela chave
-- MAGIC
-- MAGIC Para cada fonte: (1) uma *view* com os dados tipados, deduplicados (fica o arquivo mais recente de cada chave)
-- MAGIC e com uma coluna `f_<regra>` por regra de qualidade; (2) a contagem de cada regra em `prata_qualidade`;
-- MAGIC (3) `MERGE` pela chave só com os registros que não violam nenhuma regra. Rodar de novo não duplica nada.
-- MAGIC
-- MAGIC | Tabela | Chave do MERGE |
-- MAGIC | --- | --- |
-- MAGIC | `prata_lugares` | `lugar_id` |
-- MAGIC | `prata_relatos` | `relato_id` |
-- MAGIC | `prata_osm` | `osm_id` |
-- MAGIC | `prata_clima` | `lugar_id` + `data_hora` |
-- MAGIC | `prata_ibge_pcd` | `variavel_id` + `categoria_id` + `periodo` + `municipio_id` |
-- MAGIC
-- MAGIC Os registros barrados dos relatos vão para `prata_relatos_quarentena`, com o motivo.
-- MAGIC Os campos de contato do OSM (telefone, e-mail, redes) **não** passam para a Prata (LGPD).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Lugares (catálogo do Inclui+)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.gs.prata_lugares (
  lugar_id          STRING,
  nome              STRING,
  tipo              STRING,
  categoria         STRING,
  bairro            STRING,
  latitude          DOUBLE,
  longitude         DOUBLE,
  recursos          ARRAY<STRING>,
  fotos_verificadas INT,
  degraus_entrada   INT,
  largura_porta_cm  INT,
  desconto          STRING,
  data_coleta       DATE,
  arquivo_origem    STRING
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rot_lugares AS
WITH base AS (
  SELECT
    id AS lugar_id,
    name AS nome,
    kind AS tipo,
    category AS categoria,
    neighborhood AS bairro,
    try_cast(lat AS DOUBLE) AS latitude,
    try_cast(lng AS DOUBLE) AS longitude,
    from_json(features, 'array<string>') AS recursos,
    try_cast(photoVerifications AS INT) AS fotos_verificadas,
    try_cast(ent.steps AS INT) AS degraus_entrada,
    try_cast(ent.doorWidthCm AS INT) AS largura_porta_cm,
    discount AS desconto,
    to_date(regexp_extract(arquivo_origem, '_([0-9]{4}-[0-9]{2}-[0-9]{2})[.]jsonl', 1)) AS data_coleta,
    arquivo_origem,
    row_number() OVER (PARTITION BY id ORDER BY arquivo_origem DESC) AS rn
  -- Na Bronze (Auto Loader) objetos e listas do JSON ficam como texto: from_json devolve a estrutura.
  FROM (SELECT *, from_json(entrance, 'steps STRING, doorWidthCm STRING') AS ent FROM workspace.gs.bronze_lugares)
)
SELECT *,
  (lugar_id IS NULL OR trim(lugar_id) = '') AS f_id_ausente,
  -- retângulo que cobre o município de São Paulo
  (latitude IS NULL OR longitude IS NULL
    OR latitude NOT BETWEEN -24.1 AND -23.3 OR longitude NOT BETWEEN -46.9 AND -46.3) AS f_coordenada_invalida,
  (categoria IS NULL OR categoria NOT IN ('alimentacao','cultura','saude','educacao','lazer','compras')) AS f_categoria_invalida
FROM base
WHERE rn = 1;

-- COMMAND ----------

INSERT INTO workspace.gs.prata_qualidade
SELECT current_timestamp(), 'prata_lugares', regra, acao, registros FROM (
  SELECT 'duplicado_removido' AS regra, 'removido' AS acao,
         (SELECT count(*) FROM workspace.gs.bronze_lugares) - count(*) AS registros FROM rot_lugares
  UNION ALL SELECT 'id_ausente', 'barrado', count_if(f_id_ausente) FROM rot_lugares
  UNION ALL SELECT 'coordenada_invalida', 'barrado', count_if(f_coordenada_invalida) FROM rot_lugares
  UNION ALL SELECT 'categoria_invalida', 'barrado', count_if(f_categoria_invalida) FROM rot_lugares
);

-- COMMAND ----------

MERGE INTO workspace.gs.prata_lugares AS alvo
USING (
  SELECT lugar_id, nome, tipo, categoria, bairro, latitude, longitude, recursos, fotos_verificadas,
         degraus_entrada, largura_porta_cm, desconto, data_coleta, arquivo_origem
  FROM rot_lugares
  WHERE NOT (f_id_ausente OR f_coordenada_invalida OR f_categoria_invalida)
) AS novo
ON alvo.lugar_id = novo.lugar_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Relatos (fonte simulada, com defeitos de propósito)
-- MAGIC
-- MAGIC `necessidade` com maiúsculas ou espaços (`"MOBILITY "`) é **tratada** (vira `mobility`) e continua valendo;
-- MAGIC vazio, nulo ou fora das 4 necessidades é barrado.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.gs.prata_relatos (
  relato_id      STRING,
  criado_em      TIMESTAMP,
  data_relato    DATE,
  usuario_id     STRING,
  autor_nome     STRING,
  autor_email    STRING,
  lugar_id       STRING,
  necessidade    STRING,
  tipo           STRING,
  nota           INT,
  fotos          INT,
  texto          STRING,
  arquivo_origem STRING
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rot_relatos AS
WITH base AS (
  SELECT
    relato_id,
    try_cast(criado_em AS TIMESTAMP) AS criado_em,
    -- dia como escrito na origem (horário de Brasília); to_date(criado_em) usaria o fuso UTC da sessão
    try_cast(substr(criado_em, 1, 10) AS DATE) AS data_relato,
    usuario_id,
    autor_nome,
    autor_email,
    lugar_id,
    lower(trim(necessidade)) AS necessidade,
    necessidade AS necessidade_bruta,
    tipo,
    try_cast(nota AS INT) AS nota,
    try_cast(fotos AS INT) AS fotos,
    texto,
    arquivo_origem,
    row_number() OVER (PARTITION BY relato_id ORDER BY arquivo_origem DESC, carregado_em DESC) AS rn
  FROM workspace.gs.bronze_relatos
)
SELECT
  b.relato_id, b.criado_em, b.data_relato, b.usuario_id, b.autor_nome, b.autor_email,
  b.lugar_id, b.necessidade, b.necessidade_bruta, b.tipo, b.nota, b.fotos, b.texto, b.arquivo_origem,
  (b.nota IS NULL) AS f_nota_nao_numerica,
  (b.nota IS NOT NULL AND b.nota NOT BETWEEN 1 AND 5) AS f_nota_fora_da_faixa,
  (l.lugar_id IS NULL) AS f_lugar_inexistente,
  (b.necessidade IS NULL OR b.necessidade NOT IN ('vision','mobility','hearing','cognitive')) AS f_necessidade_invalida,
  (b.autor_email IS NULL OR NOT (b.autor_email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$')) AS f_email_invalido,
  (b.criado_em IS NULL) AS f_sem_data_hora
FROM base b
LEFT JOIN workspace.gs.prata_lugares l ON l.lugar_id = b.lugar_id
WHERE b.rn = 1;

-- COMMAND ----------

INSERT INTO workspace.gs.prata_qualidade
SELECT current_timestamp(), 'prata_relatos', regra, acao, registros FROM (
  SELECT 'duplicado_removido' AS regra, 'removido' AS acao,
         (SELECT count(*) FROM workspace.gs.bronze_relatos) - count(*) AS registros FROM rot_relatos
  UNION ALL SELECT 'necessidade_normalizada', 'tratado',
         count_if(NOT f_necessidade_invalida AND necessidade <> necessidade_bruta) FROM rot_relatos
  UNION ALL SELECT 'nota_nao_numerica', 'barrado', count_if(f_nota_nao_numerica) FROM rot_relatos
  UNION ALL SELECT 'nota_fora_da_faixa', 'barrado', count_if(f_nota_fora_da_faixa) FROM rot_relatos
  UNION ALL SELECT 'lugar_inexistente', 'barrado', count_if(f_lugar_inexistente) FROM rot_relatos
  UNION ALL SELECT 'necessidade_invalida', 'barrado', count_if(f_necessidade_invalida) FROM rot_relatos
  UNION ALL SELECT 'email_invalido', 'barrado', count_if(f_email_invalido) FROM rot_relatos
  UNION ALL SELECT 'sem_data_hora', 'barrado', count_if(f_sem_data_hora) FROM rot_relatos
);

-- COMMAND ----------

-- Quarentena: tudo que violou alguma regra, com os motivos (recriada a cada execução).
CREATE OR REPLACE TABLE workspace.gs.prata_relatos_quarentena AS
SELECT relato_id, usuario_id, lugar_id, necessidade_bruta, tipo, arquivo_origem,
       concat_ws(',',
         if(f_nota_nao_numerica, 'nota_nao_numerica', NULL),
         if(f_nota_fora_da_faixa, 'nota_fora_da_faixa', NULL),
         if(f_lugar_inexistente, 'lugar_inexistente', NULL),
         if(f_necessidade_invalida, 'necessidade_invalida', NULL),
         if(f_email_invalido, 'email_invalido', NULL),
         if(f_sem_data_hora, 'sem_data_hora', NULL)) AS motivos
FROM rot_relatos
WHERE f_nota_nao_numerica OR f_nota_fora_da_faixa OR f_lugar_inexistente
   OR f_necessidade_invalida OR f_email_invalido OR f_sem_data_hora;

-- COMMAND ----------

MERGE INTO workspace.gs.prata_relatos AS alvo
USING (
  SELECT relato_id, criado_em, data_relato, usuario_id, autor_nome, autor_email, lugar_id, necessidade,
         tipo, nota, fotos, texto, arquivo_origem
  FROM rot_relatos
  WHERE NOT (f_nota_nao_numerica OR f_nota_fora_da_faixa OR f_lugar_inexistente
          OR f_necessidade_invalida OR f_email_invalido OR f_sem_data_hora)
) AS novo
ON alvo.relato_id = novo.relato_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. OpenStreetMap (acessibilidade dos lugares da cidade)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.gs.prata_osm (
  osm_id         STRING,
  osm_tipo       STRING,
  nome           STRING,
  categoria      STRING,
  subtipo        STRING,
  cadeira_rodas  STRING,
  latitude       DOUBLE,
  longitude      DOUBLE,
  data_coleta    DATE,
  arquivo_origem STRING
);

-- COMMAND ----------

-- "tags" vira um mapa chave/valor: tag que não existe vira NULL em vez de erro.
-- Só passam as tags necessárias; telefone, e-mail e redes sociais ficam de fora de propósito.
CREATE OR REPLACE TEMP VIEW rot_osm AS
WITH base AS (
  SELECT
    concat(type, '/', id) AS osm_id,
    type AS osm_tipo,
    t['name'] AS nome,
    CASE
      WHEN t['amenity'] IN ('restaurant','cafe') THEN 'alimentacao'
      WHEN t['amenity'] IN ('cinema','theatre') OR t['tourism'] IN ('museum','gallery') THEN 'cultura'
      WHEN t['amenity'] IN ('clinic','doctors','hospital','pharmacy') OR t['leisure'] = 'fitness_centre' THEN 'saude'
      WHEN t['amenity'] IN ('school','library') THEN 'educacao'
      WHEN t['leisure'] = 'park' THEN 'lazer'
      WHEN t['amenity'] = 'marketplace' OR t['shop'] IN ('supermarket','books','mall') THEN 'compras'
    END AS categoria,
    coalesce(t['amenity'], t['tourism'], t['shop'], t['leisure']) AS subtipo,
    lower(trim(t['wheelchair'])) AS cadeira_rodas,
    coalesce(try_cast(lat AS DOUBLE), try_cast(c.lat AS DOUBLE)) AS latitude,
    coalesce(try_cast(lon AS DOUBLE), try_cast(c.lon AS DOUBLE)) AS longitude,
    to_date(regexp_extract(arquivo_origem, '_([0-9]{4}-[0-9]{2}-[0-9]{2})[.]jsonl', 1)) AS data_coleta,
    arquivo_origem,
    row_number() OVER (PARTITION BY type, id ORDER BY arquivo_origem DESC) AS rn
  FROM (SELECT *, from_json(tags, 'map<string,string>') AS t, from_json(center, 'lat STRING, lon STRING') AS c
        FROM workspace.gs.bronze_osm)
)
SELECT *,
  (latitude IS NULL OR longitude IS NULL) AS f_sem_coordenada,
  (cadeira_rodas IS NULL OR cadeira_rodas NOT IN ('yes','limited','no','designated')) AS f_cadeira_rodas_invalido,
  (categoria IS NULL) AS f_sem_categoria
FROM base
WHERE rn = 1;

-- COMMAND ----------

INSERT INTO workspace.gs.prata_qualidade
SELECT current_timestamp(), 'prata_osm', regra, acao, registros FROM (
  SELECT 'duplicado_removido' AS regra, 'removido' AS acao,
         (SELECT count(*) FROM workspace.gs.bronze_osm) - count(*) AS registros FROM rot_osm
  UNION ALL SELECT 'sem_coordenada', 'barrado', count_if(f_sem_coordenada) FROM rot_osm
  UNION ALL SELECT 'cadeira_rodas_invalido', 'barrado', count_if(f_cadeira_rodas_invalido) FROM rot_osm
  UNION ALL SELECT 'sem_categoria', 'barrado', count_if(f_sem_categoria) FROM rot_osm
);

-- COMMAND ----------

MERGE INTO workspace.gs.prata_osm AS alvo
USING (
  SELECT osm_id, osm_tipo, nome, categoria, subtipo, cadeira_rodas, latitude, longitude, data_coleta, arquivo_origem
  FROM rot_osm
  WHERE NOT (f_sem_coordenada OR f_cadeira_rodas_invalido OR f_sem_categoria)
) AS novo
ON alvo.osm_id = novo.osm_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Clima (a API devolve listas paralelas por hora: `arrays_zip` + `explode`)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.gs.prata_clima (
  lugar_id       STRING,
  data_hora      TIMESTAMP,
  data           DATE,
  temperatura_c  DOUBLE,
  chuva_mm       DOUBLE,
  data_coleta    DATE,
  arquivo_origem STRING
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rot_clima AS
WITH horas AS (
  SELECT lugar_id, arquivo_origem,
         explode(arrays_zip(h.time, h.temperature_2m, h.precipitation)) AS z
  FROM (SELECT *, from_json(hourly, 'time array<string>, temperature_2m array<string>, precipitation array<string>') AS h
        FROM workspace.gs.bronze_clima)
),
base AS (
  SELECT
    lugar_id,
    try_cast(z.time AS TIMESTAMP) AS data_hora,
    try_cast(z.temperature_2m AS DOUBLE) AS temperatura_c,
    try_cast(z.precipitation AS DOUBLE) AS chuva_mm,
    to_date(regexp_extract(arquivo_origem, '_([0-9]{4}-[0-9]{2}-[0-9]{2})[.]jsonl', 1)) AS data_coleta,
    arquivo_origem,
    row_number() OVER (PARTITION BY lugar_id, z.time ORDER BY arquivo_origem DESC) AS rn
  FROM horas
)
SELECT *, to_date(data_hora) AS data,
  (temperatura_c IS NULL) AS f_temperatura_nula,
  (temperatura_c IS NOT NULL AND temperatura_c NOT BETWEEN -10 AND 50) AS f_temperatura_fora_da_faixa,
  (chuva_mm IS NULL OR chuva_mm < 0) AS f_chuva_invalida
FROM base
WHERE rn = 1;

-- COMMAND ----------

INSERT INTO workspace.gs.prata_qualidade
SELECT current_timestamp(), 'prata_clima', regra, acao, registros FROM (
  SELECT 'duplicado_removido' AS regra, 'removido' AS acao,
         (SELECT sum(size(from_json(hourly, 'time array<string>').time)) FROM workspace.gs.bronze_clima) - count(*) AS registros FROM rot_clima
  UNION ALL SELECT 'temperatura_nula', 'barrado', count_if(f_temperatura_nula) FROM rot_clima
  UNION ALL SELECT 'temperatura_fora_da_faixa', 'barrado', count_if(f_temperatura_fora_da_faixa) FROM rot_clima
  UNION ALL SELECT 'chuva_invalida', 'barrado', count_if(f_chuva_invalida) FROM rot_clima
);

-- COMMAND ----------

MERGE INTO workspace.gs.prata_clima AS alvo
USING (
  SELECT lugar_id, data_hora, data, temperatura_c, chuva_mm, data_coleta, arquivo_origem
  FROM rot_clima
  WHERE NOT (f_temperatura_nula OR f_temperatura_fora_da_faixa OR f_chuva_invalida) AND data_hora IS NOT NULL
) AS novo
ON alvo.lugar_id = novo.lugar_id AND alvo.data_hora = novo.data_hora
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. IBGE (pessoas com deficiência, Censo 2022)
-- MAGIC
-- MAGIC `categoria` e `serie` vêm do JSON como objetos de chave variável (`{"73771": "..."}`, `{"2022": "400673"}`):
-- MAGIC viram mapas e pegamos a primeira chave e o primeiro valor.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.gs.prata_ibge_pcd (
  municipio_id   STRING,
  variavel_id    STRING,
  variavel       STRING,
  categoria_id   STRING,
  categoria      STRING,
  necessidade    STRING,
  periodo        INT,
  pessoas        BIGINT,
  arquivo_origem STRING
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW rot_ibge AS
WITH linhas AS (
  SELECT id AS variavel_id, variavel, arquivo_origem,
         explode(from_json(resultados,
           'array<struct<classificacoes:array<struct<categoria:map<string,string>>>,series:array<struct<localidade:struct<id:string>,serie:map<string,string>>>>>')) AS r
  FROM workspace.gs.bronze_ibge
),
mapas AS (
  SELECT variavel_id, variavel, arquivo_origem,
         r.series[0].localidade.id AS municipio_id,
         r.classificacoes[0].categoria AS cat,
         r.series[0].serie AS serie
  FROM linhas
),
base AS (
  SELECT
    municipio_id, variavel_id, variavel,
    map_keys(cat)[0] AS categoria_id,
    map_values(cat)[0] AS categoria,
    CASE map_keys(cat)[0]
      WHEN '73771' THEN 'vision' WHEN '73772' THEN 'hearing' WHEN '73773' THEN 'mobility'
      WHEN '73790' THEN 'cognitive' WHEN '73789' THEN 'manipulacao' WHEN '58771' THEN 'total'
    END AS necessidade,
    try_cast(map_keys(serie)[0] AS INT) AS periodo,
    try_cast(map_values(serie)[0] AS BIGINT) AS pessoas,
    arquivo_origem,
    row_number() OVER (
      PARTITION BY municipio_id, variavel_id, map_keys(cat)[0], map_keys(serie)[0]
      ORDER BY arquivo_origem DESC) AS rn
  FROM mapas
)
SELECT *,
  (pessoas IS NULL OR pessoas < 0) AS f_pessoas_invalido,
  (periodo IS NULL OR periodo NOT BETWEEN 1990 AND 2100) AS f_periodo_invalido,
  (necessidade IS NULL) AS f_categoria_desconhecida
FROM base
WHERE rn = 1;

-- COMMAND ----------

INSERT INTO workspace.gs.prata_qualidade
SELECT current_timestamp(), 'prata_ibge_pcd', regra, acao, registros FROM (
  SELECT 'duplicado_removido' AS regra, 'removido' AS acao,
         (SELECT sum(size(from_json(resultados, 'array<string>'))) FROM workspace.gs.bronze_ibge) - count(*) AS registros FROM rot_ibge
  UNION ALL SELECT 'pessoas_nulo_ou_negativo', 'barrado', count_if(f_pessoas_invalido) FROM rot_ibge
  UNION ALL SELECT 'periodo_invalido', 'barrado', count_if(f_periodo_invalido) FROM rot_ibge
  UNION ALL SELECT 'categoria_desconhecida', 'barrado', count_if(f_categoria_desconhecida) FROM rot_ibge
);

-- COMMAND ----------

MERGE INTO workspace.gs.prata_ibge_pcd AS alvo
USING (
  SELECT municipio_id, variavel_id, variavel, categoria_id, categoria, necessidade, periodo, pessoas, arquivo_origem
  FROM rot_ibge
  WHERE NOT (f_pessoas_invalido OR f_periodo_invalido OR f_categoria_desconhecida)
) AS novo
ON alvo.municipio_id = novo.municipio_id AND alvo.variavel_id = novo.variavel_id
   AND alvo.categoria_id = novo.categoria_id AND alvo.periodo = novo.periodo
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quanto cada regra barrou nesta execução

-- COMMAND ----------

-- Última medição de cada regra (a tabela guarda o histórico de todas as execuções).
SELECT tabela, regra, acao, registros, execucao
FROM (
  SELECT *, row_number() OVER (PARTITION BY tabela, regra ORDER BY execucao DESC) AS ordem
  FROM workspace.gs.prata_qualidade
)
WHERE ordem = 1
ORDER BY tabela, acao, regra;
