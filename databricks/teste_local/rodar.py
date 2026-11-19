"""Roda os notebooks SQL da pasta databricks/ num Spark+Delta local (Bronze simulada a partir dos .jsonl)."""
import re
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder.master("local[2]").appName("gs-teste")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "/tmp/wh")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.sources.default", "delta")
    .config("spark.sql.legacy.createHiveTableByDefault", "false")
    .config("spark.sql.shuffle.partitions", "4")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

FONTES = {"lugares": "bronze_lugares", "osm_acessibilidade": "bronze_osm", "ibge_pcd": "bronze_ibge",
          "clima": "bronze_clima", "relatos": "bronze_relatos"}
SAL = "'sal-de-teste-123'"


def bronze(datas):
    for fonte, tabela in FONTES.items():
        caminhos = [f"/data/{fonte}/{fonte}_{d}.jsonl" for d in datas]
        # Imita o Auto Loader sem inferColumnTypes: toda coluna do topo vira STRING e objetos/listas
        # ficam como texto JSON (por isso a Prata usa from_json).
        bruto = spark.read.text(caminhos).withColumn("arquivo_origem", F.input_file_name())
        mapa = bruto.select("arquivo_origem", F.from_json("value", "map<string,string>").alias("m"))
        chaves = sorted(r[0] for r in mapa.select(F.explode(F.map_keys("m")).alias("k")).distinct().collect())
        df = (mapa.select(*[F.col("m")[k].alias(k) for k in chaves], "arquivo_origem")
              .withColumn("carregado_em", F.current_timestamp()))
        df.write.format("delta").mode("append").saveAsTable(f"gs.{tabela}")


def celulas(arquivo):
    texto = open(arquivo, encoding="utf-8").read()
    linhas = [l for l in texto.splitlines() if not l.startswith("-- MAGIC") and not l.startswith("-- Databricks notebook")]
    blocos = "\n".join(linhas).split("-- COMMAND ----------")
    for b in blocos:
        sql = b.strip().rstrip(";").strip()
        sql = sql.replace("workspace.gs.", "gs.").replace(":salt_lgpd", SAL)
        corpo = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--")).strip()
        if corpo and not corpo.upper().startswith("CREATE WIDGET"):
            yield sql, corpo


def rodar(arquivo, mostrar=False):
    for sql, corpo in celulas(arquivo):
        try:
            df = spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            print("\n##### ERRO NA CELULA:\n", sql[:600], "\n#####", str(e)[:1500])
            raise
        if mostrar and corpo.upper().startswith(("SELECT", "WITH")):
            print("\n>>>", corpo.splitlines()[0][:90])
            df.show(40, truncate=70)


def contagens(titulo):
    print(f"\n=== {titulo}")
    for t in ["prata_lugares", "prata_relatos", "prata_relatos_quarentena", "prata_osm", "prata_clima", "prata_ibge_pcd"]:
        print(f"{t:28} {spark.table('gs.' + t).count()}")


spark.sql("CREATE DATABASE IF NOT EXISTS gs")
spark.sql("CREATE TABLE IF NOT EXISTS gs.prata_qualidade (execucao TIMESTAMP, tabela STRING, regra STRING, acao STRING, registros BIGINT)")

modo = sys.argv[1]
if modo == "incremental":
    bronze(["2026-11-12", "2026-11-13", "2026-11-14"])
    print("bronze lote 1:", {t: spark.table(f"gs.{t}").count() for t in FONTES.values()})
    rodar("/work/databricks/02_prata.sql")
    contagens("PRATA apos lote 1")
    bronze(["2026-11-15", "2026-11-16"])
    print("bronze lote 2:", {t: spark.table(f"gs.{t}").count() for t in FONTES.values()})
    rodar("/work/databricks/02_prata.sql")
    contagens("PRATA apos lote 2")
    rodar("/work/databricks/02_prata.sql")
    contagens("PRATA apos reexecutar (deve ser igual)")
    print("\n=== QUALIDADE da ultima execucao")
    spark.sql("""SELECT tabela, regra, acao, registros FROM (SELECT *, row_number() OVER (PARTITION BY tabela, regra ORDER BY execucao DESC) o FROM gs.prata_qualidade) WHERE o=1 ORDER BY tabela, acao, regra""").show(60, truncate=50)
    rodar("/work/databricks/03_ouro.sql", mostrar=True)
    rodar("/work/databricks/04_consultas.sql", mostrar=True)
    for t in ["ouro_dim_lugar", "ouro_dim_necessidade", "ouro_dim_tempo", "ouro_dim_usuario", "ouro_fato_relato",
              "ouro_fato_clima_dia", "ouro_osm_cobertura", "ouro_mvp_nota_lugar"]:
        print(f"{t:26} {spark.table('gs.' + t).count()}")
    print("\nSchema ouro_dim_usuario:", spark.table("gs.ouro_dim_usuario").columns)
    print("Schema ouro_fato_relato:", spark.table("gs.ouro_fato_relato").columns)
