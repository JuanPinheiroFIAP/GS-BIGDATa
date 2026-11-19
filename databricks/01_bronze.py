# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Bronze: Auto Loader (incremental, sem duplicar)
# MAGIC
# MAGIC Lê os arquivos `.jsonl` do Volume e grava uma tabela Delta por fonte, **sem transformar nada**:
# MAGIC as colunas ficam como texto (o Auto Loader não infere tipos em JSON por padrão) e o que não
# MAGIC cabe no esquema vai para `_rescued_data`. Cada linha ganha `arquivo_origem` e `carregado_em`.
# MAGIC
# MAGIC O Auto Loader guarda no *checkpoint* quais arquivos já leu. Por isso:
# MAGIC * **2º lote**: entram só os arquivos novos.
# MAGIC * **Rodar de novo** sem arquivo novo: nada entra (`novos = 0`).
# MAGIC
# MAGIC A tabela `controle_carga` guarda as contagens antes e depois de cada execução (evidência para o relatório).

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"
SCHEMA = "workspace.gs"

# pasta no Volume -> tabela Bronze
FONTES = {
    "lugares": "bronze_lugares",
    "osm_acessibilidade": "bronze_osm",
    "ibge_pcd": "bronze_ibge",
    "clima": "bronze_clima",
    "relatos": "bronze_relatos",
}


def total(tabela: str) -> int:
    nome = f"{SCHEMA}.{tabela}"
    return spark.table(nome).count() if spark.catalog.tableExists(nome) else 0


antes = {tabela: total(tabela) for tabela in FONTES.values()}
antes

# COMMAND ----------

def carregar(fonte: str, tabela: str) -> None:
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # coluna nova vai para _rescued_data, sem derrubar a carga
        .option("pathGlobFilter", "*.jsonl")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream.option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)  # processa o que está novo e termina
        .toTable(f"{SCHEMA}.{tabela}")
        .awaitTermination()
    )


for fonte, tabela in FONTES.items():
    print(f"Carregando {fonte} -> {tabela}")
    carregar(fonte, tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagens antes e depois

# COMMAND ----------

linhas = [(tabela, antes[tabela], total(tabela)) for tabela in FONTES.values()]
controle = (
    spark.createDataFrame(linhas, "tabela string, antes long, depois long")
    .withColumn("novos", F.col("depois") - F.col("antes"))
    .withColumn("execucao", F.current_timestamp())
)
controle.write.mode("append").saveAsTable(f"{SCHEMA}.controle_carga")
display(controle)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linhas por arquivo
# MAGIC Cada arquivo aparece uma única vez por tabela, mesmo depois de várias execuções.

# COMMAND ----------

partes = [
    spark.table(f"{SCHEMA}.{tabela}")
    .groupBy("arquivo_origem")
    .count()
    .withColumn("tabela", F.lit(tabela))
    for tabela in FONTES.values()
]
por_arquivo = partes[0]
for df in partes[1:]:
    por_arquivo = por_arquivo.unionByName(df)
display(por_arquivo.orderBy("tabela", "arquivo_origem"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico das execuções

# COMMAND ----------

display(spark.table(f"{SCHEMA}.controle_carga").orderBy(F.col("execucao").desc(), "tabela"))
