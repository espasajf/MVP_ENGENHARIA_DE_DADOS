# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Análise da Qualidade dos Dados
# MAGIC %md
# MAGIC # Análise da Qualidade dos Dados
# MAGIC
# MAGIC Análise de qualidade das tabelas do schema `pescador.layer_silver`:
# MAGIC
# MAGIC - `pescador.layer_silver.pescadores` (consolidada — MA, AM e PA)
# MAGIC - `pescador.layer_silver.seguro_defeso`
# MAGIC
# MAGIC Verifica: contagem de registros, valores nulos, duplicidades, integridade referencial entre tabelas e consistência de domínios. As análises de Pescadores usam a coluna `UF` para segmentar por estado.

# COMMAND ----------

# DBTITLE 1,Visão geral das tabelas
# Visão geral: total de registros e colunas por tabela
tabelas = [
    "pescador.layer_silver.pescadores",
    "pescador.layer_silver.seguro_defeso",
]

print(f"{'Tabela':<50} {'Registros':>12} {'Colunas':>8}")
print("-" * 70)
for t in tabelas:
    df = spark.table(t)
    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"{t:<50} {n_rows:>12,} {n_cols:>8}")

# Registros por UF na tabela pescadores
print("\nRegistros por UF em pescadores:")
spark.table("pescador.layer_silver.pescadores").groupBy("UF").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Nulos - Tabelas Pescadores
# Análise de valores nulos na tabela pescadores
df_pescadores = spark.table("pescador.layer_silver.pescadores")
total = df_pescadores.count()
print(f"{'=' * 60}")
print(f"Tabela: pescadores (silver) — {total:,} registros")
print(f"{'=' * 60}")
for col in df_pescadores.columns:
    nulos = df_pescadores.filter(f"{col} IS NULL").count()
    pct = (nulos / total * 100) if total > 0 else 0
    flag = " <-- ATENÇÃO" if pct > 5 else ""
    print(f"  {col:<30} {nulos:>8,} nulos ({pct:>5.1f}%){flag}")

# COMMAND ----------

# DBTITLE 1,Nulos - SeguroDefeso
# Análise de valores nulos na tabela seguro_defeso (silver)
print(f"{'=' * 60}")
print(f"Tabela: seguro_defeso (silver)")
print(f"{'=' * 60}")
df_sd = spark.table("pescador.layer_silver.seguro_defeso")
total = df_sd.count()
for col in df_sd.columns:
    nulos = df_sd.filter(f"{col} IS NULL").count()
    pct = (nulos / total * 100) if total > 0 else 0
    flag = " <-- ATENÇÃO" if pct > 5 else ""
    print(f"  {col:<45} {nulos:>8,} nulos ({pct:>5.1f}%){flag}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pescador.layer_silver.seguro_defeso

# COMMAND ----------

# DBTITLE 1,Duplicidades
    # Detecção de registros duplicados
from pyspark.sql import functions as F

print("=" * 60)
print("ANÁLISE DE DUPLICIDADES")
print("=" * 60)

# pescadores: duplicidade geral e por UF
df_pesc = spark.table("pescador.layer_silver.pescadores")
total_pesc = df_pesc.count()
distintos_geral = df_pesc.select("UF", "CPF").distinct().count()
dup_geral = total_pesc - distintos_geral
print(f"\npescadores (silver):")
print(f"  Total de registros: {total_pesc:,}")
print(f"  Comb. distintas (UF+CPF): {distintos_geral:,}")
print(f"  Duplicados: {dup_geral:,} ({dup_geral/total_pesc*100:.1f}%)")

# Duplicidade por UF
print(f"\n  Por UF:")
df_pesc.groupBy("UF").agg(
    F.count("*").alias("total"),
    F.countDistinct("CPF").alias("cpfs_distintos")
).orderBy("UF").show()

# seguro_defeso: duplicidade por combinação MES + CPF + NIS
df_sd = spark.table("pescador.layer_silver.seguro_defeso")
total_sd = df_sd.count()
distintos_sd = df_sd.select("MES_REFERENCIA", "CPF_FAVORECIDO", "NIS_FAVORECIDO").distinct().count()
dup_sd = total_sd - distintos_sd
print(f"\nseguro_defeso (silver):")
print(f"  Total de registros: {total_sd:,}")
print(f"  Comb. distintas (MES+CPF+NIS): {distintos_sd:,}")
print(f"  Duplicados: {dup_sd:,} ({dup_sd/total_sd*100:.1f}%)")



# COMMAND ----------

# DBTITLE 1,Domínios categóricos
# Análise de domínios: valores distintos em colunas categoricas da tabela pescadores
df_pesc = spark.table("pescador.layer_silver.pescadores")

categoricas = ["UF", "Sexo", "Categoria", "Forma_de_Atuacao", "Nivel_Escolaridade", "Tipo_Alfabetizacao", "UF_ESTADO", "UF_NOME"]

print("=" * 60)
print("DOMÍNIOS - pescadores (silver)")
print("=" * 60)
for col in categoricas:
    print(f"\n{col}:")
    df_pesc.groupBy(col).count().orderBy("count", ascending=False).show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Integridade referencial
# Integridade referencial: CPFs do seguro_defeso vs pescadores
print("=" * 60)
print("INTEGRIDADE REFERENCIAL - CPF no Seguro Defeso")
print("=" * 60)

df_pesc = spark.table("pescador.layer_silver.pescadores")
df_sd = spark.table("pescador.layer_silver.seguro_defeso")
cpfs_pescadores = df_pesc.select("CPF").distinct()

cpf_nulo = df_sd.filter("CPF_FAVORECIDO IS NULL").count()
print(f"\nRegistros do seguro_defeso com CPF nulo: {cpf_nulo:,}")

cpfs_sd = df_sd.filter("CPF_FAVORECIDO IS NOT NULL").select("CPF_FAVORECIDO").distinct()
cpfs_nao_cadastrados = cpfs_sd.join(cpfs_pescadores, cpfs_sd.CPF_FAVORECIDO == cpfs_pescadores.CPF, "left_anti")
qtd_nao_cad = cpfs_nao_cadastrados.count()
total_cpf_distintos = cpfs_sd.count()
pct_nao = qtd_nao_cad / total_cpf_distintos * 100 if total_cpf_distintos > 0 else 0
print(f"CPFs distintos no seguro_defeso: {total_cpf_distintos:,}")
print(f"CPFs não encontrados no cadastro de pescadores: {qtd_nao_cad:,} ({pct_nao:.1f}%)")

# Integridade referencial: municípios em seguro_defeso vs pescadores
print("\n" + "=" * 60)
print("INTEGRIDADE REFERENCIAL - Município no Seguro Defeso")
print("=" * 60)

municipios_pesc = df_pesc.select("Municipio").distinct()
municipios_sd = df_sd.filter("NOME_MUNICIPIO IS NOT NULL").select("NOME_MUNICIPIO").distinct()
municipios_nao_encontrados = municipios_sd.join(
    municipios_pesc,
    municipios_sd.NOME_MUNICIPIO == municipios_pesc.Municipio,
    "left_anti"
)
qtd_mun_nao = municipios_nao_encontrados.count()
total_mun_sd = municipios_sd.count()
pct_mun = qtd_mun_nao / total_mun_sd * 100 if total_mun_sd > 0 else 0
print(f"Municípios distintos no seguro_defeso: {total_mun_sd:,}")
print(f"Municípios não encontrados em pescadores: {qtd_mun_nao:,} ({pct_mun:.1f}%)")
if qtd_mun_nao > 0:
    print(f"\nExemplos de municípios não encontrados:")
    municipios_nao_encontrados.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Anomalias - Dt Nascimento
# Verificacao de anomalias em Dt_Nascimento (anos invalidos) na tabela pescadores
from pyspark.sql import functions as F

print("=" * 60)
print("ANOMALIAS - Data de Nascimento (pescadores)")
print("=" * 60)

df_pesc = spark.table("pescador.layer_silver.pescadores")
total = df_pesc.count()
nulos = df_pesc.filter("Dt_Nascimento IS NULL").count()
invalidos = df_pesc.filter("Dt_Nascimento < 1900 OR Dt_Nascimento > 2025").count()
print(f"\n  Total: {total:,}")
print(f"  Dt_Nascimento nula: {nulos:,}")
print(f"  Ano < 1900 ou > 2025: {invalidos:,}")
if invalidos > 0:
    df_pesc.filter("Dt_Nascimento < 1900 OR Dt_Nascimento > 2025").select("UF", "Dt_Nascimento", "Nome_do_Pescador").show(10, truncate=False)

# Anomalias por UF
print(f"\n  Por UF:")
df_pesc.groupBy("UF").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("Dt_Nascimento").isNull(), 1).otherwise(0)).alias("dt_nula"),
    F.sum(F.when((F.col("Dt_Nascimento") < 1900) | (F.col("Dt_Nascimento") > 2025), 1).otherwise(0)).alias("invalidas")
).orderBy("UF").show()

# COMMAND ----------

# DBTITLE 1,Anomalias - Valor Parcela
# Estatisticas de VALOR_PARCELA no SeguroDefeso
from pyspark.sql import functions as F

print("=" * 60)
print("ANOMALIAS - VALOR_PARCELA no seguro_defeso")
print("=" * 60)

df_sd = spark.table("pescador.layer_silver.seguro_defeso")
df_sd.selectExpr(
    "min(VALOR_PARCELA) as min_valor",
    "max(VALOR_PARCELA) as max_valor",
    "round(avg(VALOR_PARCELA), 2) as avg_valor",
    "count(*) as total",
    "sum(case when VALOR_PARCELA IS NULL then 1 else 0 end) as nulos",
    "sum(case when VALOR_PARCELA <= 0 then 1 else 0 end) as zeros_ou_neg"
).show()

# Distribuicao por mes de referencia
print("\nDistribuicao por MES_REFERENCIA:")
df_sd.groupBy("MES_REFERENCIA").agg(
    F.count("*").alias("registros"),
    F.round(F.sum("VALOR_PARCELA"), 2).alias("valor_total"),
    F.countDistinct("CPF_FAVORECIDO").alias("cpfs_distintos")
).orderBy("MES_REFERENCIA").show()

# COMMAND ----------

# DBTITLE 1,Resumo consolidado
# Resumo consolidado das verificacoes de qualidade
from pyspark.sql import functions as F

print("#" * 60)
print("RESUMO CONSOLIDADO - QUALIDADE DOS DADOS (SILVER)")
print("#" * 60)

# pescadores resumo
df_pesc = spark.table("pescador.layer_silver.pescadores")
total = df_pesc.count()
cpf_nulos = df_pesc.filter("CPF IS NULL").count()
cpf_distintos = df_pesc.select("CPF").distinct().count()
dup = total - cpf_distintos
dt_nula = df_pesc.filter("Dt_Nascimento IS NULL").count()

print(f"\n{'Tabela':<20} {'Registros':>10} {'CPF nulo':>10} {'Dup. CPF':>10} {'Dt nasc. nulo':>14}")
print("-" * 64)
print(f"{'pescadores':<20} {total:>10,} {cpf_nulos:>10,} {dup:>10,} {dt_nula:>14,}")

# seguro_defeso resumo
df_sd = spark.table("pescador.layer_silver.seguro_defeso")
sd_total = df_sd.count()
sd_cpf_nulo = df_sd.filter("CPF_FAVORECIDO IS NULL").count()
print(f"{'seguro_defeso':<20} {sd_total:>10,} {sd_cpf_nulo:>10,}")

# Resumo pescadores por UF
print(f"\nRegistros por UF em pescadores:")
df_pesc.groupBy("UF").agg(
    F.count("*").alias("registros"),
    F.countDistinct("CPF").alias("cpfs_distintos")
).orderBy("UF").show()

print("\nNota: Verificar as células acima para detalhes de nulos, domínios e integridade referencial.")