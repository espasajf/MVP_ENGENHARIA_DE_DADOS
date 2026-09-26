# Databricks notebook source
# DBTITLE 1,Importação Relatório DTB Brasil Município
# MAGIC %md
# MAGIC # Importação Relatório DTB Brasil Município
# MAGIC
# MAGIC Carrega o arquivo `/Volumes/pescador/staging/dados_rgp/Dados/RELATORIO_DTB_BRASIL_MUNICIPIO.xls` para a tabela `pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO`. Exibe o total de registros, colunas e uma amostra dos dados.

# COMMAND ----------

# DBTITLE 1,Ler XLS e criar tabela
# MAGIC %pip install xlrd
# MAGIC
# MAGIC import re
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Caminho do arquivo XLS
# MAGIC xls_path = "/Volumes/pescador/staging/dados_rgp/Dados/RELATORIO_DTB_BRASIL_MUNICIPIO.xls"
# MAGIC
# MAGIC # Le o XLS com pandas (engine xlrd para formato .xls antigo)
# MAGIC df_pd = pd.read_excel(xls_path, engine="xlrd")
# MAGIC print(f"Arquivo lido: {xls_path}")
# MAGIC print(f"Shape: {df_pd.shape}")
# MAGIC print(f"Colunas originais: {list(df_pd.columns)}")
# MAGIC
# MAGIC # Sanitiza nomes de colunas (remove acentos, espacos e caracteres especiais)
# MAGIC def sanitize_col(name):
# MAGIC     name = str(name).strip()
# MAGIC     for old, new in {"ç": "c", "ã": "a", "á": "a", "é": "e", "í": "i", "ó": "o",
# MAGIC                      "ú": "u", "â": "a", "ê": "e", "ô": "o", "Ç": "C", "Ã": "A",
# MAGIC                      "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U"}.items():
# MAGIC         name = name.replace(old, new)
# MAGIC     name = re.sub(r"[^A-Za-z0-9_]", "_", name)
# MAGIC     name = re.sub(r"_+", "_", name)
# MAGIC     return name.strip("_")
# MAGIC
# MAGIC df_pd.columns = [sanitize_col(c) for c in df_pd.columns]
# MAGIC print(f"Colunas sanitizadas: {list(df_pd.columns)}")
# MAGIC
# MAGIC # Converte para Spark DataFrame
# MAGIC df_spark = spark.createDataFrame(df_pd)
# MAGIC
# MAGIC # Cria/sobrescreve a tabela
# MAGIC df_spark.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO")
# MAGIC
# MAGIC print("Tabela pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra
# Resumo da tabela criada
total_registros = spark.table("pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO").count()
total_colunas = len(spark.table("pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO").columns)

print("=" * 60)
print(f"Tabela: pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO")
print(f"Total de registros: {total_registros:,}")
print(f"Total de colunas: {total_colunas}")
print("=" * 60)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO").limit(10))

# COMMAND ----------

# DBTITLE 1,Importação Códigos UF IBGE
# MAGIC %md
# MAGIC # Importação Códigos UF IBGE
# MAGIC
# MAGIC Carrega o arquivo `/Volumes/pescador/staging/dados_rgp/Dados/codigos_uf_ibge.xlsx` para a tabela `pescador.layer_bronze.CODIGOS_UF_IBGE`. O arquivo contém 27 linhas e 3 colunas (`Codigo_UF`, `Sigla_UF`, `Nome_UF`) já sanitizadas. Exibe o total de registros, colunas e uma amostra dos dados.

# COMMAND ----------

# DBTITLE 1,Ler XLSX e criar tabela
# MAGIC %pip install openpyxl
# MAGIC
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Caminho do arquivo XLSX
# MAGIC xlsx_path = "/Volumes/pescador/staging/dados_rgp/Dados/codigos_uf_ibge.xlsx"
# MAGIC
# MAGIC # Le o XLSX com pandas
# MAGIC df_pd = pd.read_excel(xlsx_path)
# MAGIC print(f"Arquivo lido: {xlsx_path}")
# MAGIC print(f"Shape: {df_pd.shape}")
# MAGIC print(f"Colunas: {list(df_pd.columns)}")
# MAGIC
# MAGIC # Converte para Spark DataFrame
# MAGIC df_spark = spark.createDataFrame(df_pd)
# MAGIC
# MAGIC # Cria/sobrescreve a tabela
# MAGIC df_spark.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("pescador.layer_bronze.CODIGOS_UF_IBGE")
# MAGIC
# MAGIC print("Tabela pescador.layer_bronze.CODIGOS_UF_IBGE criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra - Códigos UF IBGE
# Resumo da tabela criada
total_registros = spark.table("pescador.layer_bronze.CODIGOS_UF_IBGE").count()
total_colunas = len(spark.table("pescador.layer_bronze.CODIGOS_UF_IBGE").columns)

print("=" * 60)
print(f"Tabela: pescador.layer_bronze.CODIGOS_UF_IBGE")
print(f"Total de registros: {total_registros:,}")
print(f"Total de colunas: {total_colunas}")
print("=" * 60)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_bronze.CODIGOS_UF_IBGE").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_bronze.CODIGOS_UF_IBGE").limit(10))

# COMMAND ----------

# DBTITLE 1,Criação Silver MUNICIPIOS
# MAGIC %md
# MAGIC # Criação da tabela Silver MUNICIPIOS
# MAGIC
# MAGIC Faz um join entre as tabelas bronze `RELATORIO_DTB_BRASIL_MUNICIPIO` e `CODIGOS_UF_IBGE` (pelo código da UF) para criar a tabela `pescador.layer_silver.MUNICIPIOS` com os campos:
# MAGIC
# MAGIC - `SIGLA_UF` — sigla da UF (ex: SP, RJ, MA)
# MAGIC - `NM_MUNICIPIO` — nome do município
# MAGIC - `CODIGO_MUNICIPIO` — código completo do município (IBGE de 7 dígitos)

# COMMAND ----------

# DBTITLE 1,Criação Silver MUNICIPIOS_BR
# MAGIC %md
# MAGIC # Criação da tabela Silver MUNICIPIOS_BR
# MAGIC
# MAGIC Cria a tabela `pescador.layer_silver.MUNICIPIOS_BR` a partir de `RELATORIO_DTB_BRASIL_MUNICIPIO` com os campos:
# MAGIC
# MAGIC - `CODIGO_MUNICIPIO` — código completo do município (IBGE de 7 dígitos)
# MAGIC - `SIGLA_UF` — sigla da UF (ex: SP, RJ, MA)
# MAGIC - `NOME_MUNICIPIO` — nome do município

# COMMAND ----------

# DBTITLE 1,Criar tabela silver MUNICIPIOS_BR
# Cria a tabela silver MUNICIPIOS_BR a partir do RELATORIO_DTB_BRASIL_MUNICIPIO
# Faz um join com CODIGOS_UF_IBGE para obter a sigla da UF
df_dtb = spark.table("pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO")
df_uf = spark.table("pescador.layer_bronze.CODIGOS_UF_IBGE")

df_silver_br = df_dtb.join(
    df_uf,
    df_dtb["UF"] == df_uf["Codigo_UF"],
    "left"
).select(
    df_dtb["Codigo_Municipio_Completo"].alias("CODIGO_MUNICIPIO"),
    df_uf["Sigla_UF"].alias("SIGLA_UF"),
    df_dtb["Nome_Municipio"].alias("NOME_MUNICIPIO"),
)

df_silver_br.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_silver.MUNICIPIOS_BR")

print("Tabela pescador.layer_silver.MUNICIPIOS_BR criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra - Silver MUNICIPIOS_BR
# Resumo da tabela silver criada
total_registros = spark.table("pescador.layer_silver.MUNICIPIOS_BR").count()
total_colunas = len(spark.table("pescador.layer_silver.MUNICIPIOS_BR").columns)

print("=" * 60)
print(f"Tabela: pescador.layer_silver.MUNICIPIOS_BR")
print(f"Total de registros: {total_registros:,}")
print(f"Total de colunas: {total_colunas}")
print("=" * 60)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_silver.MUNICIPIOS_BR").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_silver.MUNICIPIOS_BR").limit(10))