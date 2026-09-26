# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Gold - Resultado Final
# MAGIC %md
# MAGIC # Gold - Tabela Resultado Final
# MAGIC
# MAGIC Cria a tabela `pescador.layer_gold.RESULTADO1` juntando dados das tabelas silver:
# MAGIC
# MAGIC - `seguro_defeso` — parcelas pagas e beneficiarios
# MAGIC - `municipios_br` — codigo do municipio (IBGE)
# MAGIC - `censo_ibge` — populacao do municipio
# MAGIC
# MAGIC **Campos resultantes:**
# MAGIC
# MAGIC | Campo | Descrição |
# MAGIC |---|---|
# MAGIC | `NM_MUNICIPIO` | Nome do município |
# MAGIC | `QTD_PARCELAS` | Quantidade de parcelas pagas (count de registros) |
# MAGIC | `VALOR_TOTAL_PAGO` | Soma de todas as parcelas |
# MAGIC | `QTD_BENEFICIARIOS` | Beneficiários distintos (count distinct CPF_PRIMEIRO_NOME) |
# MAGIC | `POPULACAO` | População do município (Censo IBGE 2022) |

# COMMAND ----------

# DBTITLE 1,Criar tabela gold RESULTADO_FINAL
from pyspark.sql.functions import upper, translate, col, count, countDistinct, sum, max as max_

# Carrega as tabelas silver
df_sd = spark.table("pescador.layer_silver.SEGURO_DEFESO")
df_mun = spark.table("pescador.layer_silver.MUNICIPIOS_BR")
df_censo = spark.table("pescador.layer_silver.CENSO_IBGE")

# Normaliza nomes de municipios: maiusculas + sem acentos
# seguro_defeso ja esta em maiusculas sem acentos; municipios_br esta em title case com acentos
acc_from = "\u00c1\u00c0\u00c3\u00c2\u00c4\u00c5\u00c7\u00c8\u00c9\u00ca\u00cb\u00cc\u00cd\u00ce\u00cf\u00d1\u00d2\u00d3\u00d4\u00d5\u00d6\u00d9\u00da\u00db\u00dc\u00dd\u00e1\u00e0\u00e3\u00e2\u00e4\u00e5\u00e7\u00e8\u00e9\u00ea\u00eb\u00ec\u00ed\u00ee\u00ef\u00f1\u00f2\u00f3\u00f4\u00f5\u00f6\u00f9\u00fa\u00fb\u00fc\u00fd"
acc_to   = "AAAAAACEEEEIIIINOOOOOUUUUYAAAAAACEEEEIIIINOOOOOUUUUY"

df_mun_norm = df_mun.withColumn(
    "NOME_NORM",
    translate(upper(col("NOME_MUNICIPIO")), acc_from, acc_to)
)
df_sd_norm = df_sd.withColumn(
    "NOME_NORM",
    translate(upper(col("NOME_MUNICIPIO")), acc_from, acc_to)
)

# Join seguro_defeso + municipios_br (por nome normalizado + UF) para obter CODIGO_MUNICIPIO
df_joined = df_sd_norm.join(
    df_mun_norm,
    (df_sd_norm["NOME_NORM"] == df_mun_norm["NOME_NORM"]) &
    (df_sd_norm["UF"] == df_mun_norm["SIGLA_UF"]),
    "left"
)

# Join com censo_ibge para obter POPULACAO
df_joined = df_joined.join(
    df_censo,
    df_joined["CODIGO_MUNICIPIO"] == df_censo["CODIGO_MUNICIPIO"],
    "left"
)

# Agrega por municipio
df_gold = df_joined.groupBy(
    df_sd_norm["NOME_MUNICIPIO"].alias("NM_MUNICIPIO"),
    df_sd_norm["UF"]
).agg(
    count("*").alias("QTD_PARCELAS"),
    sum("VALOR_PARCELA").alias("VALOR_TOTAL_PAGO"),
    countDistinct("CPF_PRIMEIRO_NOME").alias("QTD_BENEFICIARIOS"),
    max_("POPULACAO").alias("POPULACAO"),
)

df_gold = df_gold.select(
    "NM_MUNICIPIO",
    "UF",
    "QTD_PARCELAS",
    "VALOR_TOTAL_PAGO",
    "QTD_BENEFICIARIOS",
    "POPULACAO",
)

df_gold.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_gold.RESULTADO1")

print("Tabela pescador.layer_gold.RESULTADO1 criada/sobrescrita com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra - Gold RESULTADO_FINAL
# Resumo da tabela gold criada
total_municipios = spark.table("pescador.layer_gold.RESULTADO1").count()

print("=" * 70)
print(f"Tabela: pescador.layer_gold.RESULTADO1")
print(f"Total de municipios: {total_municipios:,}")
print("=" * 70)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_gold.RESULTADO1").printSchema()

# Top 10 municipios por valor total pago
print("\nTop 10 municipios por valor total pago:")
display(
    spark.table("pescador.layer_gold.RESULTADO1")
    .orderBy(col("VALOR_TOTAL_PAGO").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC uf,
# MAGIC sum(qtd_beneficiarios) as qtd_beneficiarios, 
# MAGIC sum(populacao) as qtd_populacao,
# MAGIC sum(qtd_beneficiarios)/sum(populacao)*100 as taxa_beneficiados
# MAGIC from pescador.layer_gold.RESULTADO1 
# MAGIC group by uf 
# MAGIC order by taxa_beneficiados desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC uf,nm_municipio,
# MAGIC sum(qtd_beneficiarios) as qtd_beneficiarios, 
# MAGIC sum(populacao) as qtd_populacao,
# MAGIC sum(qtd_beneficiarios)/sum(populacao)*100 as taxa_beneficiados
# MAGIC from pescador.layer_gold.RESULTADO1 
# MAGIC where uf in ('MA','PA','AP','AM','AC')
# MAGIC group by uf,nm_municipio
# MAGIC order by taxa_beneficiados desc

# COMMAND ----------

# DBTITLE 1,Gold - Resultado 2
# MAGIC %md
# MAGIC # Gold - Tabela Resultado 2
# MAGIC
# MAGIC Cria a tabela `pescador.layer_gold.RESULTADO_2` com um join entre `seguro_defeso` e `pescadores` pelo campo `CPF_PRIMEIRO_NOME`.
# MAGIC
# MAGIC **Campos resultantes:**
# MAGIC
# MAGIC | Campo | Origem |
# MAGIC |---|---|
# MAGIC | `CPF_PRIMEIRO_NOME` | seguro_defeso / pescadores (chave de join) |
# MAGIC | `UF_SEGURO` | seguro_defeso.UF |
# MAGIC | `NM_MUNICIPIO_SEGURO` | seguro_defeso.NOME_MUNICIPIO |
# MAGIC | `UF_PESCADOR` | pescadores.UF |
# MAGIC | `NM_MUNICIPIO_PESCADOR` | pescadores.Municipio |

# COMMAND ----------

# DBTITLE 1,Criar tabela gold RESULTADO_2
# Join entre seguro_defeso e pescadores pelo CPF_PRIMEIRO_NOME
# Nota: seguro_defeso usa formato '334895FRANCISCO' (sem underscore)
# enquanto pescadores usa '824423_LOOHANNY' (com underscore). Normalizamos ambos.
from pyspark.sql.functions import regexp_replace as rr

df_sd = spark.table("pescador.layer_silver.SEGURO_DEFESO")
df_pesc = spark.table("pescador.layer_silver.PESCADORES")

# Seleciona e normaliza as colunas necessarias de cada tabela
df_sd_sel = df_sd.select(
    rr(col("CPF_PRIMEIRO_NOME"), "_", "").alias("CPF_PRIMEIRO_NOME"),
    col("UF").alias("UF_SEGURO"),
    col("NOME_MUNICIPIO").alias("NM_MUNICIPIO_SEGURO"),
).filter("CPF_PRIMEIRO_NOME IS NOT NULL")

df_pesc_sel = df_pesc.select(
    rr(col("CPF_PRIMEIRO_NOME"), "_", "").alias("CPF_PRIMEIRO_NOME"),
    col("UF").alias("UF_PESCADOR"),
    col("Municipio").alias("NM_MUNICIPIO_PESCADOR"),
).filter("CPF_PRIMEIRO_NOME IS NOT NULL")

# Join pelo CPF_PRIMEIRO_NOME normalizado (inner join: apenas CPFs que existem em ambas)
df_resultado2 = df_sd_sel.join(
    df_pesc_sel,
    df_sd_sel["CPF_PRIMEIRO_NOME"] == df_pesc_sel["CPF_PRIMEIRO_NOME"],
    "inner"
)

# Seleciona e reorganiza as colunas finais
df_resultado2 = df_resultado2.select(
    df_sd_sel["CPF_PRIMEIRO_NOME"],
    col("UF_SEGURO"),
    col("NM_MUNICIPIO_SEGURO"),
    col("UF_PESCADOR"),
    col("NM_MUNICIPIO_PESCADOR"),
)

df_resultado2.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pescador.layer_gold.RESULTADO_2")

print("Tabela pescador.layer_gold.RESULTADO_2 criada/sobrescrita com sucesso!")
print(f"Total de registros: {df_resultado2.count():,}")

# COMMAND ----------

# DBTITLE 1,Resumo e amostra - Gold RESULTADO_2
# Resumo da tabela gold criada
total_registros = spark.table("pescador.layer_gold.RESULTADO_2").count()

print("=" * 70)
print(f"Tabela: pescador.layer_gold.RESULTADO_2")
print(f"Total de registros: {total_registros:,}")
print("=" * 70)

# Schema da tabela
print(f"\nSchema:")
spark.table("pescador.layer_gold.RESULTADO_2").printSchema()

# Amostra dos dados
display(spark.table("pescador.layer_gold.RESULTADO_2").limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(distinct cpf_primeiro_nome) 
# MAGIC from pescador.layer_gold.resultado_2 
# MAGIC where nm_municipio_seguro <> nm_municipio_pescador and uf_seguro <> uf_pescador

# COMMAND ----------

# DBTITLE 1,Análise RESULTADO_2
# MAGIC %md
# MAGIC # Análise da tabela RESULTADO_2
# MAGIC
# MAGIC Análise exploratória da tabela `pescador.layer_gold.RESULTADO_2` que cruza dados de seguro_defeso com pescadores pelo CPF_PRIMEIRO_NOME.

# COMMAND ----------

# DBTITLE 1,Visão geral RESULTADO_2
# Visão geral da tabela
df = spark.table("pescador.layer_gold.RESULTADO_2")

total = df.count()
distintos = df.select("CPF_PRIMEIRO_NOME").distinct().count()
uf_seguro_distintas = df.select("UF_SEGURO").distinct().count()
uf_pescador_distintas = df.select("UF_PESCADOR").distinct().count()
mun_seguro_distintos = df.select("NM_MUNICIPIO_SEGURO").distinct().count()
mun_pescador_distintos = df.select("NM_MUNICIPIO_PESCADOR").distinct().count()

print("=" * 70)
print("VISÃO GERAL - pescador.layer_gold.RESULTADO_2")
print("=" * 70)
print(f"  Total de registros:          {total:,}")
print(f"  CPFs distintos:              {distintos:,}")
print(f"  UFs distintas (seguro):      {uf_seguro_distintas}")
print(f"  UFs distintas (pescador):    {uf_pescador_distintas}")
print(f"  Municipios distintos (seguro):  {mun_seguro_distintos:,}")
print(f"  Municipios distintos (pescador): {mun_pescador_distintos:,}")
print(f"  Media de parcelas por CPF:    {total / distintos:.1f}")

print("\nDistribuicao por UF_SEGURO:")
df.groupBy("UF_SEGURO").count().orderBy(col("count").desc()).show(30, truncate=False)

print("\nDistribuicao por UF_PESCADOR:")
df.groupBy("UF_PESCADOR").count().orderBy(col("count").desc()).show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Compatibilidade UF RESULTADO_2
# Análise de compatibilidade: UF do seguro vs UF do pescador
from pyspark.sql.functions import col, count, desc, when

print("=" * 70)
print("COMPATIBILIDADE UF_SEGURO vs UF_PESCADOR")
print("=" * 70)

# Quantos registros têm UF iguais vs diferentes
iguais = df.filter(col("UF_SEGURO") == col("UF_PESCADOR")).count()
diferentes = df.filter(col("UF_SEGURO") != col("UF_PESCADOR")).count()
print(f"  UF igual (mesmo estado):     {iguais:,} ({iguais/total*100:.1f}%)")
print(f"  UF diferente (estado divergente): {diferentes:,} ({diferentes/total*100:.1f}%)")

print("\nCruzamento UF_SEGURO x UF_PESCADOR (top 20 combinacoes):")
df.groupBy("UF_SEGURO", "UF_PESCADOR").count().orderBy(desc("count")).show(20, truncate=False)

print("\nDivergencias de UF (top 20):")
df.filter(col("UF_SEGURO") != col("UF_PESCADOR")) \
    .groupBy("UF_SEGURO", "UF_PESCADOR").count() \
    .orderBy(desc("count")).show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Compatibilidade Município RESULTADO_2
# Análise de compatibilidade: Municipio do seguro vs Municipio do pescador
print("=" * 70)
print("COMPATIBILIDADE MUNICIPIO_SEGURO vs MUNICIPIO_PESCADOR")
print("=" * 70)

# Normaliza para comparar (uppercase + sem acentos)
from pyspark.sql.functions import upper, translate

acc_from = "\u00c1\u00c0\u00c3\u00c2\u00c4\u00c5\u00c7\u00c8\u00c9\u00ca\u00cb\u00cc\u00cd\u00ce\u00cf\u00d1\u00d2\u00d3\u00d4\u00d5\u00d6\u00d9\u00da\u00db\u00dc\u00dd\u00e1\u00e0\u00e3\u00e2\u00e4\u00e5\u00e7\u00e8\u00e9\u00ea\u00eb\u00ec\u00ed\u00ee\u00ef\u00f1\u00f2\u00f3\u00f4\u00f5\u00f6\u00f9\u00fa\u00fb\u00fc\u00fd"
acc_to   = "AAAAAACEEEEIIIINOOOOOUUUUYAAAAAACEEEEIIIINOOOOOUUUUY"

df_cmp = df.withColumn(
    "MUN_SEGURO_NORM",
    translate(upper(col("NM_MUNICIPIO_SEGURO")), acc_from, acc_to)
).withColumn(
    "MUN_PESCADOR_NORM",
    translate(upper(col("NM_MUNICIPIO_PESCADOR")), acc_from, acc_to)
)

mun_iguais = df_cmp.filter(col("MUN_SEGURO_NORM") == col("MUN_PESCADOR_NORM")).count()
mun_diferentes = df_cmp.filter(col("MUN_SEGURO_NORM") != col("MUN_PESCADOR_NORM")).count()
print(f"  Mesmo municipio:          {mun_iguais:,} ({mun_iguais/total*100:.1f}%)")
print(f"  Municipio divergente:     {mun_diferentes:,} ({mun_diferentes/total*100:.1f}%)")

print("\nTop 15 divergencias de municipio (seguro vs pescador):")
df_cmp.filter(col("MUN_SEGURO_NORM") != col("MUN_PESCADOR_NORM")) \
    .groupBy("NM_MUNICIPIO_SEGURO", "UF_SEGURO", "NM_MUNICIPIO_PESCADOR", "UF_PESCADOR") \
    .count().orderBy(desc("count")).show(15, truncate=False)

# COMMAND ----------

# DBTITLE 1,Top municípios RESULTADO_2
# Top 15 municipios com mais pescadores recebendo seguro defeso
print("=" * 70)
print("TOP 15 MUNICIPIOS (origem pescador) POR QTD DE PARCELAS")
print("=" * 70)

top_mun = df.groupBy("UF_PESCADOR", "NM_MUNICIPIO_PESCADOR") \
    .agg(
        count("*").alias("qtd_parcelas"),
        countDistinct("CPF_PRIMEIRO_NOME").alias("qtd_beneficiarios"),
    ) \
    .orderBy(desc("qtd_parcelas")) \
    .limit(15)

display(top_mun)