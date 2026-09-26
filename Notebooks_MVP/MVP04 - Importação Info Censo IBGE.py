# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Importação Censo IBGE 2022
# MAGIC %md
# MAGIC # Importação Informações Censo IBGE 2022
# MAGIC
# MAGIC Carrega o arquivo `/Volumes/pescador/staging/dados_rgp/Dados/br_ibge_censo_2022_municipio.csv` para a tabela `pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO`. Exibe o total de registros, colunas e uma amostra dos dados.

# COMMAND ----------

# DBTITLE 1,Ler CSV e criar tabela
import pandas as pd

# Caminho do arquivo CSV
csv_path = "/Volumes/pescador/staging/dados_rgp/Dados/br_ibge_censo_2022_municipio.csv"

# Le o CSV: encoding latin-1, separador virgula
df_pd = pd.read_csv(csv_path, sep=",", encoding="latin-1")
print(f"Arquivo lido: {csv_path}")
print(f"Shape: {df_pd.shape}")
print(f"Colunas: {list(df_pd.columns)}")

# Converte para Spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# Cria/sobrescreve a tabela
df_spark.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO")

print("Tabela pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra
# Resumo da tabela criada
total_registros = spark.table("pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO").count()
total_colunas = len(spark.table("pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO").columns)

print("=" * 60)
print(f"Tabela: pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO")
print(f"Total de registros: {total_registros:,}")
print(f"Total de colunas: {total_colunas}")
print("=" * 60)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO").limit(10))

# COMMAND ----------

# DBTITLE 1,Criação Silver CENSO_IBGE
# MAGIC %md
# MAGIC # Criação da tabela Silver CENSO_IBGE
# MAGIC
# MAGIC Cria a tabela `pescador.layer_silver.CENSO_IBGE` a partir de `IBGE_CENSO_2022_MUNICIPIO` com os campos:
# MAGIC
# MAGIC - `CODIGO_MUNICIPIO` — código do município (IBGE)
# MAGIC - `POPULACAO` — população do município segundo o censo 2022

# COMMAND ----------

# DBTITLE 1,Criar tabela silver CENSO_IBGE
# Cria a tabela silver CENSO_IBGE a partir do IBGE_CENSO_2022_MUNICIPIO
df_bronze = spark.table("pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO")

df_silver = df_bronze.select(
    df_bronze["id_municipio"].alias("CODIGO_MUNICIPIO"),
    df_bronze["populacao"].alias("POPULACAO"),
)

df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_silver.CENSO_IBGE")

print("Tabela pescador.layer_silver.CENSO_IBGE criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra - Silver CENSO_IBGE
# Resumo da tabela silver criada
total_registros = spark.table("pescador.layer_silver.CENSO_IBGE").count()
total_colunas = len(spark.table("pescador.layer_silver.CENSO_IBGE").columns)

print("=" * 60)
print(f"Tabela: pescador.layer_silver.CENSO_IBGE")
print(f"Total de registros: {total_registros:,}")
print(f"Total de colunas: {total_colunas}")
print("=" * 60)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_silver.CENSO_IBGE").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_silver.CENSO_IBGE").limit(10))