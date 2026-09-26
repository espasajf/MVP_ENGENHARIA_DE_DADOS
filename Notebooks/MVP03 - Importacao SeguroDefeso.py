# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Importação Seguro Defeso - RGP
# MAGIC %md
# MAGIC # Importação Seguro Defeso - RGP
# MAGIC
# MAGIC Carrega todos os arquivos CSV `YYYYMM_SeguroDefeso.csv` do diretório `/Volumes/pescador/staging/dados_rgp/Dados/`.
# MAGIC
# MAGIC **Camada Bronze:** Cada arquivo (competência YYYYMM) é salvo como uma tabela separada em `pescador.layer_bronze` (ex: `seguro_defeso_202503`, `seguro_defeso_202504`, ...).
# MAGIC
# MAGIC **Camada Bronze:** Consolidação de todos os períodos em uma única tabela `pescador.layer_bronze.SEGURO_DEFESO_2025_2026`. Exibe o total de registros, colunas e um resumo por arquivo.

# COMMAND ----------

# DBTITLE 1,Config e funcoes utilitarias
import os
import re
import pandas as pd

# Diretório com os arquivos CSV
csv_dir = "/Volumes/pescador/staging/dados_rgp/Dados"

# Lista apenas arquivos *_SeguroDefeso.csv (ignora subdiretErios e outros CSVs)
csv_files = sorted([f for f in os.listdir(csv_dir) if f.endswith("_SeguroDefeso.csv")])
print(f"Arquivos encontrados ({len(csv_files)}):")
for f in csv_files:
    size_mb = os.path.getsize(os.path.join(csv_dir, f)) / (1024 * 1024)
    print(f"  - {f} ({size_mb:.1f} MB)")


def sanitize_col(name):
    """Sanitiza nomes de colunas para o Delta (sem espacos, acentos ou caracteres especiais)."""
    name = name.strip()
    for old, new in {"ç": "c", "ã": "a", "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "â": "a", "ê": "e", "ô": "o", "û": "u", "Ç": "C", "Ã": "A", "Á": "A", "É": "E", "Ê": "E", "Í": "I", "Ó": "O", "Ô": "O", "Ú": "U", "Â": "A"}.items():
        name = name.replace(old, new)
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")

# COMMAND ----------

# DBTITLE 1,Ler e processar CSVs
# Processa cada arquivo CSV: le com pandas, sanitiza colunas, converte VALOR PARCELA
resultados = []
dfs_spark = []

for filename in csv_files:
    filepath = os.path.join(csv_dir, filename)
    print(f"\nProcessando: {filename}")

    # Le o CSV: encoding latin-1, separador ';'
    df_pd = pd.read_csv(filepath, sep=";", encoding="latin-1", dtype=str)

    # Converte VALOR PARCELA de string (vírgula decimal) para float
    if "VALOR PARCELA" in df_pd.columns:
        df_pd["VALOR PARCELA"] = df_pd["VALOR PARCELA"].str.replace(",", ".").astype(float)

    # Converte MÊS REFERÊNCIA para int
    if "MÊS REFERÊNCIA" in df_pd.columns:
        df_pd["MÊS REFERÊNCIA"] = df_pd["MÊS REFERÊNCIA"].astype(int)

    # Converte CÓDIGO MUNICÍPIO SIAFI para int
    if "CÓDIGO MUNICÍPIO SIAFI" in df_pd.columns:
        df_pd["CÓDIGO MUNICÍPIO SIAFI"] = df_pd["CÓDIGO MUNICÍPIO SIAFI"].astype(int)

    # Converte NIS FAVORECIDO para int
    if "NIS FAVORECIDO" in df_pd.columns:
        df_pd["NIS FAVORECIDO"] = pd.to_numeric(df_pd["NIS FAVORECIDO"], errors="coerce")

    # Sanitiza nomes de colunas
    df_pd.columns = [sanitize_col(c) for c in df_pd.columns]

    # Converte para Spark DataFrame
    df_spark = spark.createDataFrame(df_pd)

    # Salva tabela parcial na camada bronze (uma por competencia)
    competencia = filename[:6]  # Extrai YYYYMM do nome do arquivo
    tabela_bronze = f"pescador.layer_bronze.seguro_defeso_{competencia}"
    df_spark.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tabela_bronze)
    print(f"  Tabela bronze criada: {tabela_bronze}")

    dfs_spark.append(df_spark)

    n_rows = df_pd.shape[0]
    n_cols = df_pd.shape[1]
    print(f"  Registros: {n_rows:,} | Colunas: {n_cols}")
    resultados.append({"arquivo": filename, "registros": n_rows, "colunas": n_cols})

print(f"\nTotal de arquivos processados: {len(resultados)}")

# COMMAND ----------

# DBTITLE 1,Unificar e criar tabela
# Unifica todos os DataFrames Spark e cria/sobrescreve a tabela
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

df_unified = reduce(DataFrame.unionByName, dfs_spark)

# Nova coluna CPF_PRIMEIRO_NOME: concatena a parte numerica do CPF
# (apenas digitos, removendo mascaras) com o primeiro nome do campo NOME_FAVORECIDO
df_unified = df_unified.withColumn(
    "CPF_PRIMEIRO_NOME",
    F.concat(
        F.regexp_replace(F.col("CPF_FAVORECIDO"), r"[^0-9]", ""),
        F.split(F.col("NOME_FAVORECIDO"), " ").getItem(0)
    )
)

# Seleciona apenas as colunas finais desejadas
df_unified = df_unified.select(
    F.col("MES_REFERENCIA"),
    F.col("UF"),
    F.col("NOME_MUNICIPIO"),
    F.col("CPF_PRIMEIRO_NOME"),
    F.col("VALOR_PARCELA"),
)

df_unified.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_bronze.SEGURO_DEFESO_2025_2026")

print("Tabela pescador.layer_bronze.SEGURO_DEFESO_2025_2026 criada/sobrescrita com sucesso!")
print("Coluna CPF_PRIMEIRO_NOME adicionada (parte numerica do CPF + primeiro nome)")

# COMMAND ----------

# DBTITLE 1,Resumo e schema
# Resumo geral da tabela unificada
total_registros = spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026").count()
total_colunas = len(spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026").columns)

# Amostra da nova coluna CPF_PRIMEIRO_NOME
print("Amostra da tabela:")
spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026").show(10, truncate=False)

print("=" * 60)
print(f"Tabela: pescador.layer_bronze.SEGURO_DEFESO_2025_2026")
print(f"Total de registros: {total_registros:,}")
print(f"Total de colunas: {total_colunas}")
print("=" * 60)

# Resumo por arquivo
print(f"\n{'Arquivo':<35} {'Registros':>12}")
print(f"{'-' * 35} {'-' * 12}")
for r in resultados:
    print(f"{r['arquivo']:<35} {r['registros']:>12,}")
print(f"{'-' * 35} {'-' * 12}")
print(f"{'TOTAL':<35} {total_registros:>12,}")

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026").printSchema()

# COMMAND ----------

# DBTITLE 1,Amostra da tabela
# Mostra amostra da tabela unificada
display(spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026").limit(20))

# COMMAND ----------

# DBTITLE 1,Analise da tabela SEGURO_DEFESO_2025_2026
# Analise da tabela SEGURO_DEFESO_2025_2026
from pyspark.sql.functions import col, count, countDistinct, sum as sum_fn, round as round_fn, desc, min as min_fn, max as max_fn

df = spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026")
total = df.count()

print("=" * 70)
print("ANALISE DA TABELA pescador.layer_bronze.SEGURO_DEFESO_2025_2026")
print("=" * 70)

# Visao geral
print(f"\n1. VISAO GERAL")
print(f"   Total de registros:           {total:,}")
print(f"   CPFs distintos (nao nulos):   {df.filter('CPF_PRIMEIRO_NOME IS NOT NULL').select('CPF_PRIMEIRO_NOME').distinct().count():,}")
print(f"   CPFs nulos:                   {df.filter('CPF_PRIMEIRO_NOME IS NULL').count():,} ({df.filter('CPF_PRIMEIRO_NOME IS NULL').count()/total*100:.1f}%)")
print(f"   Municipios distintos:         {df.select('NOME_MUNICIPIO').distinct().count():,}")
print(f"   UFs distintas:                {df.select('UF').distinct().count()}")
print(f"   Competencias (meses):          {df.select('MES_REFERENCIA').distinct().count()}")

# Estatisticas de VALOR_PARCELA
print(f"\n2. ESTATISTICAS DE VALOR_PARCELA")
df.selectExpr(
    "min(VALOR_PARCELA) as min_valor",
    "max(VALOR_PARCELA) as max_valor",
    "round(avg(VALOR_PARCELA), 2) as avg_valor",
    "round(sum(VALOR_PARCELA), 2) as soma_total",
    "sum(case when VALOR_PARCELA IS NULL then 1 else 0 end) as nulos",
    "sum(case when VALOR_PARCELA <= 0 then 1 else 0 end) as zeros_ou_neg"
).show()

# Distribuicao por UF
print(f"\n3. DISTRIBUICAO POR UF (top 15)")
display(
    df.groupBy("UF")
      .agg(
          count("*").alias("qtd_parcelas"),
          countDistinct("CPF_PRIMEIRO_NOME").alias("cpfs_distintos"),
          round_fn(sum_fn("VALOR_PARCELA"), 2).alias("valor_total")
      )
      .orderBy(desc("qtd_parcelas"))
      .limit(15)
)

# Distribuicao por mes de referencia
print(f"\n4. DISTRIBUICAO POR MES DE REFERENCIA")
display(
    df.groupBy("MES_REFERENCIA")
      .agg(
          count("*").alias("qtd_parcelas"),
          round_fn(sum_fn("VALOR_PARCELA"), 2).alias("valor_total"),
          countDistinct("CPF_PRIMEIRO_NOME").alias("cpfs_distintos")
      )
      .orderBy("MES_REFERENCIA")
)

# Top 10 municipios por valor total
print(f"\n5. TOP 10 MUNICIPIOS POR VALOR TOTAL PAGO")
display(
    df.groupBy("UF", "NOME_MUNICIPIO")
      .agg(
          count("*").alias("qtd_parcelas"),
          countDistinct("CPF_PRIMEIRO_NOME").alias("beneficiarios"),
          round_fn(sum_fn("VALOR_PARCELA"), 2).alias("valor_total")
      )
      .orderBy(desc("valor_total"))
      .limit(10)
)

# Valores anomalos (<= 0)
print(f"\n6. VALORES ANOMALOS (VALOR_PARCELA <= 0)")
anomalos = df.filter(col("VALOR_PARCELA") <= 0).count()
print(f"   Registros com valor <= 0: {anomalos:,}")
if anomalos > 0:
    df.filter(col("VALOR_PARCELA") <= 0).groupBy("VALOR_PARCELA").count().orderBy("VALOR_PARCELA").show(10, truncate=False)

# Verificacao de valores nulos por coluna
print(f"\n7. VALORES NULOS POR COLUNA")
print(f"{'Coluna':<25} {'Nulos':>10} {'% Nulos':>10}")
print("-" * 45)
for c in df.columns:
    nulos = df.filter(col(c).isNull()).count()
    pct = nulos / total * 100 if total > 0 else 0
    flag = " <-- ATENCAO" if pct > 5 else ""
    print(f"{c:<25} {nulos:>10,} {pct:>9.1f}%{flag}")

# COMMAND ----------

# DBTITLE 1,Criação Silver SEGURO_DEFESO
# MAGIC %md
# MAGIC # Criação da tabela Silver SEGURO_DEFESO
# MAGIC
# MAGIC Cria a tabela `pescador.layer_silver.seguro_defeso` a partir de `pescador.layer_bronze.SEGURO_DEFESO_2025_2026`, excluindo os registros onde `CPF_PRIMEIRO_NOME` é nulo.

# COMMAND ----------

# DBTITLE 1,Criar tabela silver seguro_defeso
# Cria a tabela silver seguro_defeso a partir do bronze, excluindo CPF_PRIMEIRO_NOME nulo
df_bronze = spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026")
df_silver = df_bronze.filter(col("CPF_PRIMEIRO_NOME").isNotNull())

df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_silver.seguro_defeso")

print("Tabela pescador.layer_silver.seguro_defeso criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra - Silver SEGURO_DEFESO
# Resumo da tabela silver criada
total_bronze = spark.table("pescador.layer_bronze.SEGURO_DEFESO_2025_2026").count()
total_silver = spark.table("pescador.layer_silver.seguro_defeso").count()
removidos = total_bronze - total_silver

total_colunas = len(spark.table("pescador.layer_silver.seguro_defeso").columns)

print("=" * 60)
print(f"Tabela: pescador.layer_silver.seguro_defeso")
print(f"Total de registros: {total_silver:,}")
print(f"Total de colunas: {total_colunas}")
print(f"Registros removidos (CPF nulo): {removidos:,}")
print(f"Bronze: {total_bronze:,} -> Silver: {total_silver:,}")
print("=" * 60)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_silver.seguro_defeso").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_silver.seguro_defeso").limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*),sum(qtd) from (select cpf_primeiro_nome,count(*) as qtd from pescador.layer_silver.seguro_defeso group by cpf_primeiro_nome having count(*) > 1)