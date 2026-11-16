# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — Alaga Alerta
# MAGIC Requisito 06: tipos corretos, nulos/inválidos tratados, **MERGE pela chave** e
# MAGIC **3 regras de qualidade com total barrado por cada uma**.
# MAGIC Requisito 08 (LGPD): dados pessoais das ocorrências **mascarados com hash** aqui.

# COMMAND ----------
CATALOG, SCHEMA = "workspace", "gs"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------
def aplicar_regras(df: DataFrame, regras: dict, rotulo: str) -> DataFrame:
    """Aplica regras de qualidade, imprime o total barrado por cada uma e retorna o df válido."""
    total = df.count()
    print(f"\n=== Qualidade · {rotulo} === (total bruto: {total})")
    validos = df
    for nome, cond in regras.items():
        barrados = validos.filter(~cond).count()
        validos = validos.filter(cond)
        print(f"  Regra '{nome}': barrados = {barrados}")
    print(f"  -> válidos: {validos.count()} / {total}")
    return validos

def merge_prata(df: DataFrame, tabela: str, chaves: list[str]):
    """UPSERT pela chave (cria a tabela na 1ª vez; MERGE nas seguintes)."""
    full = f"{CATALOG}.{SCHEMA}.{tabela}"
    if not spark.catalog.tableExists(full):
        df.write.saveAsTable(full)
        print(f"[{tabela}] criada com {df.count()} linhas")
        return
    df.createOrReplaceTempView("novo")
    on = " AND ".join([f"alvo.{k} = novo.{k}" for k in chaves])
    spark.sql(f"""
        MERGE INTO {full} AS alvo USING novo
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[{tabela}] MERGE concluído")

# COMMAND ----------
# MAGIC %md ## Prata · Clima (explode das listas paralelas do Open-Meteo)
# COMMAND ----------
reg = (spark.table("bronze_clima")
       .select(F.explode("registros").alias("r"))
       .select("r.bairro_id", "r.bairro", "r.hourly"))

zipped = reg.select(
    "bairro_id", "bairro",
    F.explode(F.arrays_zip(
        F.col("hourly.time"), F.col("hourly.precipitation"), F.col("hourly.temperature_2m")
    )).alias("h"))

clima = (zipped.select(
            "bairro_id", "bairro",
            F.to_timestamp("h.time").alias("data_hora"),
            F.col("h.precipitation").cast("double").alias("precipitacao_mm"),
            F.col("h.temperature_2m").cast("double").alias("temperatura_c"))
         .dropDuplicates(["bairro_id", "data_hora"]))

clima_ok = aplicar_regras(clima, {
    "data_hora_valida": F.col("data_hora").isNotNull(),
    "precipitacao_nao_negativa": F.col("precipitacao_mm") >= 0,
    "bairro_id_presente": F.col("bairro_id").isNotNull(),
}, "clima")

merge_prata(clima_ok, "prata_clima", ["bairro_id", "data_hora"])

# COMMAND ----------
# MAGIC %md ## Prata · Ocorrências — MASCARAMENTO LGPD (hash SHA-256)
# COMMAND ----------
oc = (spark.table("bronze_ocorrencias")
      .select(F.explode("ocorrencias").alias("o")).select("o.*"))

oc_mask = oc.select(
    "protocolo", "bairro_id",
    F.to_timestamp("data_hora").alias("data_hora"),
    # dados pessoais -> substituídos por hash irreversível (LGPD, req. 08)
    F.sha2(F.col("nome"), 256).alias("nome_hash"),
    F.sha2(F.col("telefone"), 256).alias("telefone_hash"),
    F.sha2(F.col("cpf"), 256).alias("cpf_hash"),
    "relato",
    F.col("nivel_agua_cm").cast("int").alias("nivel_agua_cm"),
).dropDuplicates(["protocolo"])

oc_ok = aplicar_regras(oc_mask, {
    "data_hora_valida": F.col("data_hora").isNotNull(),
    "nivel_agua_valido": (F.col("nivel_agua_cm").isNotNull()) & (F.col("nivel_agua_cm") >= 0),
    "bairro_id_presente": F.col("bairro_id").isNotNull(),
}, "ocorrencias")

merge_prata(oc_ok, "prata_ocorrencias", ["protocolo"])

# COMMAND ----------
# MAGIC %md ## Prata · População (extrai o total do município do payload IBGE)
# COMMAND ----------
pop_raw = spark.table("bronze_populacao")
# payload IBGE: resultados[0].series[0].serie = {"<ano>": "<valor>"}
pop = (pop_raw
       .select("data_coleta", F.explode("payload").alias("v"))
       .select("data_coleta",
               F.col("v.resultados")[0]["series"][0]["serie"].alias("serie"))
       .select("data_coleta", F.map_values("serie")[0].cast("long").alias("populacao_municipio_sp")))

pop_ok = aplicar_regras(pop, {
    "populacao_positiva": F.col("populacao_municipio_sp") > 0,
    "data_coleta_presente": F.col("data_coleta").isNotNull(),
    "populacao_nao_nula": F.col("populacao_municipio_sp").isNotNull(),
}, "populacao").dropDuplicates(["data_coleta"])

merge_prata(pop_ok, "prata_populacao", ["data_coleta"])

# COMMAND ----------
display(spark.sql("SELECT * FROM prata_clima ORDER BY data_hora DESC LIMIT 20"))
# Confirme que prata_ocorrencias NÃO tem nome/telefone/cpf em claro — só *_hash.
display(spark.sql("SELECT * FROM prata_ocorrencias LIMIT 10"))
