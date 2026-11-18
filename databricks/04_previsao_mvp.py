# Databricks notebook source
# MAGIC %md
# MAGIC # Previsão vigente para o MVP
# MAGIC Execute após os notebooks 01 a 03. Envie o JSON de previsão para a pasta `previsao/`
# MAGIC no Volume `workspace.gs.chegada`. Esta trilha mantém previsão e observação em tabelas próprias.

# COMMAND ----------
from pyspark.sql import Window, functions as F

esquema = "workspace.gs"
volume = "/Volumes/workspace/gs/chegada"
bronze = f"{esquema}.bronze_previsao"
prata = f"{esquema}.prata_previsao"
ouro = f"{esquema}.fato_previsao_bairro_hora"
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md ## Bronze: arquivos incrementais com checkpoint

# COMMAND ----------
antes = spark.table(bronze).count() if spark.catalog.tableExists(bronze) else 0
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema("bairro_id STRING, data_hora STRING, precipitacao_mm DOUBLE, coletado_em STRING")
    .load(f"{volume}/previsao")
    .withColumn("arquivo_origem", F.col("_metadata.file_path"))
    .writeStream.option("checkpointLocation", f"{volume}/_controle/checkpoint_previsao")
    .trigger(availableNow=True).toTable(bronze).awaitTermination())
depois = spark.table(bronze).count()
print(f"Bronze previsão: antes={antes}, depois={depois}, novos={depois - antes}")

# COMMAND ----------
# MAGIC %md ## Prata: última previsão por bairro e hora

# COMMAND ----------
convertidos = (spark.table(bronze)
    .withColumn("data_hora", F.expr("try_cast(data_hora AS TIMESTAMP)"))
    .withColumn("coletado_em", F.expr("try_cast(coletado_em AS TIMESTAMP)")))
validos = convertidos.filter(
    F.col("data_hora").isNotNull() & F.col("coletado_em").isNotNull()
    & F.col("precipitacao_mm").between(0, 500) & F.col("bairro_id").isNotNull()
)
print(f"Previsões inválidas na Bronze: {depois - validos.count()}")
janela = Window.partitionBy("bairro_id", "data_hora").orderBy(
    F.desc("coletado_em"), F.desc("arquivo_origem")
)
(validos.withColumn("_ordem", F.row_number().over(janela))
    .where("_ordem = 1").drop("_ordem")
    .createOrReplaceTempView("previsao_validada"))
spark.sql(f"CREATE TABLE IF NOT EXISTS {prata} AS SELECT * FROM previsao_validada WHERE false")
spark.sql(f"""
    MERGE INTO {prata} AS atual
    USING previsao_validada AS nova
    ON atual.bairro_id = nova.bairro_id AND atual.data_hora = nova.data_hora
    WHEN MATCHED AND nova.coletado_em >= atual.coletado_em THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %md ## Ouro: risco, população potencialmente exposta e exportação para a API

# COMMAND ----------
spark.sql(f"""
    CREATE OR REPLACE VIEW {ouro} AS
    SELECT p.bairro_id, b.bairro, p.data_hora, p.precipitacao_mm, p.coletado_em,
        CASE WHEN p.precipitacao_mm < 2 THEN 'normal'
             WHEN p.precipitacao_mm < 10 THEN 'atencao'
             WHEN p.precipitacao_mm < 25 THEN 'alto' ELSE 'critico' END AS nivel_risco,
        CASE WHEN p.precipitacao_mm >= 10 THEN b.populacao ELSE 0 END AS populacao_exposta
    FROM {prata} p JOIN {esquema}.dim_bairro b ON b.bairro_id = p.bairro_id
""")

vigente = spark.sql(f"""
    SELECT bairro_id, date_format(data_hora, "yyyy-MM-dd'T'HH:mm:ssXXX") AS data_hora,
           precipitacao_mm, nivel_risco
    FROM {ouro}
    WHERE data_hora >= date_trunc('HOUR', current_timestamp())
      AND data_hora < date_trunc('HOUR', current_timestamp()) + INTERVAL 12 HOURS
      AND coletado_em >= current_timestamp() - INTERVAL 3 HOURS
    ORDER BY bairro_id, data_hora
""")
if not vigente.take(1):
    raise ValueError("Nenhuma previsão vigente. Colete e envie um lote novo antes de exportar.")
display(vigente)
(vigente.coalesce(1).write.mode("overwrite").option("header", "true")
    .csv(f"{volume}/exportacao_mvp"))
print(f"CSV da Ouro salvo em {volume}/exportacao_mvp")
