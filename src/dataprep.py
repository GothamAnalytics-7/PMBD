# Importações Básicas
import pyspark
# import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)
import pandas as pd
import numpy as np
import os
import sys
import math
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import matplotlib.pyplot as plt
import pyarrow

def dataprep(spark: SparkSession, main_dir: str, parquet_dir: str, sample_size: float):
    # # Inicializa a sessão Spark
    # spark = SparkSession.builder.appName("PMBD - Data Preparation and Data Engineering") \
    #     .config("spark.sql.shuffle.partitions", "400") \
    #     .config("spark.driver.maxResultSize", "4g") \
    #     .config("spark.sql.execution.arrow.enabled", "true") \
    #     .config("spark.driver.memory", "4g") \
    #     .config("spark.executor.cores", "8") \
    #     .config("spark.executor.memoryOverhead", "4g") \
    #     .config("spark.executor.instances", "10") \
    #     .getOrCreate()

    # spark.sparkContext.setLogLevel("INFO")  # Pode ser ajustado para "INFO" durante o desenvolvimento

    # data_dir = "../data/"

    # dataProcessed_dir = data_dir + "processed/"
    # dataRaw_dir = data_dir + "raw/"
    # # Criar diretórios se não existirem
    # os.makedirs(dataProcessed_dir, exist_ok=True)
    # os.makedirs(dataRaw_dir, exist_ok=True)

    # allData = False

    # schema = StructType([
    #     StructField("event_time", TimestampType(), True),
    #     StructField("event_type", StringType(), True),
    #     StructField("product_id", LongType(), True),
    #     StructField("category_id", LongType(), True),
    #     StructField("category_code", StringType(), True),
    #     StructField("brand", StringType(), True),
    #     StructField("price", DoubleType(), True),
    #     StructField("user_id", LongType(), True),
    #     StructField("user_session", StringType(), True)
    # ])

    # if allData:
    #     # Lê o arquivo parquet completo - não recomendado para grandes volumes de dados
    #     df = spark.read.parquet(dataRaw_dir + "ec_total.parquet", schema=schema)
    # else:
    #     # Lê o arquivo parquet em partes - recomendado para grandes volumes de dados
    #     df_nov = spark.read.csv(dataRaw_dir + '2019-Nov.csv', schema=schema, header=True).limit(1000000)
    #     # df_oct = spark.read.csv(dataRaw_dir + '2019-Oct.csv', schema=schema, header=True)
    ec = spark.read.parquet(parquet_dir)
    # Remove observações com categoria nula
    ec_clean = ec.filter(~isnull(col("category_code"))).filter(~isnan(col("category_code")))

    # Separação do category_code através do '.'
    split_col = split(ec_clean['category_code'], '\.')

    # Separação do category_code em categoria e sub categorias
    ec_clean = ec_clean.withColumn('main_category', split_col.getItem(0)) \
        .withColumn('sub_category_1', split_col.getItem(1)) \
        .withColumn('sub_category_2', split_col.getItem(2))

    # Remoção da category_code original 
    ec_clean = ec_clean.drop('category_code')

    top_10_cat = ec_clean.filter(col("event_type") == "view") \
    .repartition(24, 'main_category')\
    .groupBy("main_category") \
    .count() \
    .orderBy("count", ascending=False) \
    .select("main_category") \
    .collect()

    top_10_cat = [x[0] for x in top_10_cat][:10]
    top_10_cat

    user_df = ec_clean.filter((col("event_type") == "view") & (col("main_category").isin(top_10_cat))) \
        .repartition(24, "user_id") \
        .groupBy("user_id") \
        .agg(
            avg("price").alias("average_price"),
            count("*").alias("views")
        ) \
        .withColumn("average_price", round(col("average_price"), 3))

    user_df.show()

    avg_price_df = ec_clean.filter((col("event_type") == "view")) \
    .groupBy("user_id") \
    .agg(round(avg("price"), 3).alias("average_price"),
        count("*").alias("total_views"))

    pivot_views_df = ec_clean.filter(
        (col("event_type") == "view") & 
        (col("main_category").isin(top_10_cat))
    ).groupBy("user_id") \
    .pivot("main_category", top_10_cat) \
    .agg(count("*"))

    user_df = avg_price_df.join(pivot_views_df, on="user_id", how="left")
    user_df = user_df.fillna(0)
    user_df.show()

    # write in a parquet file
    user_df.write.parquet(main_dir + f"processed/user_df_{sample_size}.parquet", mode="overwrite")