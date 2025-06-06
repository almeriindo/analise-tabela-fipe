import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, collect_list, avg
import pandas as pd
from pyspark.sql.functions import trim

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Inicializar Spark
spark = SparkSession.builder.appName("DadosTabelaFife").getOrCreate()

# Caminho do arquivo
arquivo_csv = "tabela-fipe-historico-precos.csv"

# Ler o CSV
df = spark.read.csv(
    arquivo_csv,
    header=True,
    inferSchema=True
)

# Renomeando colunas
df = df.withColumnRenamed("", "Índice") \
    .withColumnRenamed("codigoFipe", "Código Fipe") \
    .withColumnRenamed("marca", "Marca") \
    .withColumnRenamed("modelo", "Modelo") \
    .withColumnRenamed("anoModelo", "Ano Modelo") \
    .withColumnRenamed("mesReferencia", "Mês Referência") \
    .withColumnRenamed("anoReferencia", "Ano Referência") \
    .withColumnRenamed("valor", "Valor")

# Filtros desejados
marcas_desejadas = ["Audi", "LAMBORGHINI", "VW - VolksWagen"]
anos_desejados = [2017, 2018, 2019, 2020, 2021, 2022]

# Aplicar filtros de marca e ano
df = df.filter(col("Marca").isin(marcas_desejadas)) \
       .filter(col("Ano Modelo").isin(anos_desejados))

# Filtrar apenas modelos que começam com letra ou número
df = df.withColumn("Modelo", trim(col("Modelo"))) \
    .filter(col("Modelo").rlike("^[a-zA-Z0-9]"))

# Extrair o primeiro nome do modelo
df = df.withColumn("Modelo Base", split(col("Modelo"), " ").getItem(0))

# Agrupar por marca, modelo_base e ano
# collect_list("modelo").alias("modelos_completos"),
df_agrupado = df.groupBy("Marca", "Modelo Base", "Ano Modelo") \
    .agg( 
        avg("valor").alias("Valor medio")
    )

# Converter para Pandas
df_pandas = df_agrupado.toPandas()

# FORMATAR a média com separador de milhar e duas casas decimais
df_pandas["Valor medio"] = df_pandas["Valor medio"].map(
    lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)

# Salvar como CSV
df_pandas.to_csv("tabela-fipe-tratado.csv", index=False)
