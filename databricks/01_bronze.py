# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Alaga Alerta
# MAGIC Requisito 05: ingestão incremental com **Auto Loader** (cloudFiles). No 2º lote só
# MAGIC entram arquivos novos; rodar de novo **não duplica** (checkpoint + schemaLocation).
# MAGIC Guardamos `arquivo_origem` e `carregado_em` e mostramos **contagens antes e depois**.

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "gs"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/chegada"   # Volume onde os lotes são enviados pela interface

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F

# COMMAND ----------
def _contar(tabela: str) -> int:
    try:
        return spark.table(tabela).count()
    except Exception:
        return 0

def ingerir_bronze(fonte: str, tabela: str):
    """Lê incrementalmente os JSON de chegada/<fonte> para a tabela bronze."""
    antes = _contar(tabela)
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .option("multiLine", "true")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.{tabela}")
        .awaitTermination())
    depois = _contar(tabela)
    print(f"[{tabela}] antes={antes}  depois={depois}  novos={depois - antes}")

# COMMAND ----------
# MAGIC %md ### Contagens ANTES (rode antes de enviar o 2º lote, para o relatório)
# COMMAND ----------
for t in ["bronze_clima", "bronze_populacao", "bronze_ocorrencias"]:
    print(f"{t}: {_contar(t)} linhas")

# COMMAND ----------
# MAGIC %md ### Ingestão incremental (Auto Loader)
# COMMAND ----------
ingerir_bronze("clima", "bronze_clima")
ingerir_bronze("populacao", "bronze_populacao")
ingerir_bronze("ocorrencias", "bronze_ocorrencias")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Prova de idempotência (req. 05)
# MAGIC Rode a célula acima **duas vezes**: na 2ª, `novos=0` em todas as tabelas —
# MAGIC o Auto Loader não reprocessa arquivos já lidos. Print disso vai no relatório.

# COMMAND ----------
display(spark.sql("""
  SELECT 'bronze_clima' AS tabela, count(*) AS linhas, count(DISTINCT arquivo_origem) AS arquivos FROM bronze_clima
  UNION ALL SELECT 'bronze_populacao', count(*), count(DISTINCT arquivo_origem) FROM bronze_populacao
  UNION ALL SELECT 'bronze_ocorrencias', count(*), count(DISTINCT arquivo_origem) FROM bronze_ocorrencias
"""))
