# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — Alaga Alerta (modelo dimensional)
# MAGIC Requisito 07: **1 fato + 2 dimensões**. O fato `fato_risco_bairro_hora` é a tabela
# MAGIC que o **MVP consome** para mostrar o risco por bairro (req. 08).
# MAGIC
# MAGIC **Classificação de risco** (precipitação mm/h): <2 normal · 2–10 atenção · 10–25 alto · ≥25 crítico.
# MAGIC **População exposta** = população do bairro quando o risco é *alto* ou *crítico* (indicador de impacto).

# COMMAND ----------
CATALOG, SCHEMA = "workspace", "gs"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## dim_bairro (referência: bairros de SP com histórico de alagamento + população/open data)
# COMMAND ----------
bairros = [
    ("SP001", "Lapa (Marginal Tietê)", "Lapa", -23.5280, -46.7050, 68848),
    ("SP002", "Vila Maria (Marginal Tietê)", "Vila Maria", -23.5120, -46.5860, 113845),
    ("SP003", "Jaguaré", "Jaguaré", -23.5460, -46.7600, 49863),
    ("SP004", "Ipiranga (Ricardo Jafet)", "Ipiranga", -23.5950, -46.6100, 108706),
    ("SP005", "Vila Prudente", "Vila Prudente", -23.5840, -46.5800, 103838),
    ("SP006", "Santo Amaro (Jurubatuba)", "Santo Amaro", -23.6550, -46.7050, 71316),
    ("SP007", "Jardim Ângela (M'Boi Mirim)", "Jardim Ângela", -23.6830, -46.7660, 295434),
    ("SP008", "Penha (Rio Tiquatira)", "Penha", -23.5330, -46.5430, 127820),
    ("SP009", "Pinheiros (Cidade Jardim)", "Pinheiros", -23.5880, -46.6970, 65364),
    ("SP010", "Butantã (Rio Pirajuçara)", "Butantã", -23.5710, -46.7280, 54196),
]
dim_bairro = spark.createDataFrame(
    bairros, ["bairro_id", "bairro", "distrito", "latitude", "longitude", "populacao"])
dim_bairro.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_bairro")

# COMMAND ----------
# MAGIC %md ## dim_tempo (derivada dos instantes observados na Prata)
# COMMAND ----------
dim_tempo = (spark.table("prata_clima").select("data_hora").distinct()
    .withColumn("data", F.to_date("data_hora"))
    .withColumn("hora", F.hour("data_hora"))
    .withColumn("dia_semana", F.date_format("data_hora", "EEEE"))
    .withColumn("periodo", F.when(F.hour("data_hora") < 6, "madrugada")
                            .when(F.hour("data_hora") < 12, "manha")
                            .when(F.hour("data_hora") < 18, "tarde")
                            .otherwise("noite")))
dim_tempo.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_tempo")

# COMMAND ----------
# MAGIC %md ## fato_risco_bairro_hora (o que o MVP mostra)
# COMMAND ----------
ocor_hora = (spark.table("prata_ocorrencias")
    .withColumn("data_hora", F.date_trunc("hour", "data_hora"))
    .groupBy("bairro_id", "data_hora")
    .agg(F.count("*").alias("qtd_ocorrencias"),
         F.max("nivel_agua_cm").alias("max_nivel_agua_cm")))

fato = (spark.table("prata_clima").alias("c")
    .join(dim_bairro.alias("b"), "bairro_id")
    .join(ocor_hora.alias("o"), ["bairro_id", "data_hora"], "left")
    .withColumn("nivel_risco",
        F.when(F.col("precipitacao_mm") < 2, "normal")
         .when(F.col("precipitacao_mm") < 10, "atencao")
         .when(F.col("precipitacao_mm") < 25, "alto")
         .otherwise("critico"))
    .withColumn("populacao_exposta",
        F.when(F.col("nivel_risco").isin("alto", "critico"), F.col("populacao")).otherwise(F.lit(0)))
    .select("bairro_id", "data_hora", "precipitacao_mm", "temperatura_c",
            "nivel_risco", "populacao_exposta",
            F.coalesce("qtd_ocorrencias", F.lit(0)).alias("qtd_ocorrencias"),
            F.coalesce("max_nivel_agua_cm", F.lit(0)).alias("max_nivel_agua_cm")))

fato.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fato_risco_bairro_hora")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Tabela que o MVP consome: `workspace.gs.fato_risco_bairro_hora`
# MAGIC Sem dados pessoais (só agregados por bairro/hora). As 5 consultas estão em `consultas_ouro.sql`.
# COMMAND ----------
display(spark.sql("""
  SELECT b.bairro, f.data_hora, f.precipitacao_mm, f.nivel_risco, f.populacao_exposta, f.qtd_ocorrencias
  FROM fato_risco_bairro_hora f JOIN dim_bairro b USING (bairro_id)
  WHERE f.nivel_risco IN ('alto','critico')
  ORDER BY f.data_hora DESC, f.populacao_exposta DESC
  LIMIT 20
"""))
