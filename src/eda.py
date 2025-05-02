import pyspark
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)
from pathlib import Path
# from ydata_profiling import ProfileReport
from pyspark.sql.functions import countDistinct, col, isnan, when, count, split, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import Bucketizer
import numpy as np
import plotly
import plotly.express as px


def eda(spark: SparkSession, parquet_dir: str):
    # Leitura de dados
    print(parquet_dir)

    # Converte para spark
    ec = spark.read.parquet(parquet_dir)

    # Vizualização da estrutura
    ec.printSchema()

    # Vizualização das primeiras 20 linhas
    print(ec.show())

    # Número de elementos únicos por categoria
    num_users = ec.select("user_id").distinct().count()
    num_products = ec.select("product_id").distinct().count()
    num_categories = ec.select("category_code").distinct().count()
    num_taxonomies = ec.select("category_id").distinct().count()
    num_brands = ec.select("brand").distinct().count()

    print('Total number of users: ', num_users)
    print('Total number of products: ', num_products)
    print('Total number of categories: ', num_categories)
    print('Total number of taxonomies: ', num_taxonomies)
    print('Total number of brands: ', num_brands)

    # Colunas separadas em uma lista
    col_ec = ec.columns
    print(f"{len(col_ec)} columns:", col_ec)

    # Contagem de observações duplicadas
    # O que é um duplicado? O mesmo utilizador, para o mesmo produto, 
    # ter o mesmo tipo de evento no mesmo momento
    print(f'Ecommerce - number of rows is {ec.count()}; after dropDuplicates() applied would be {ec.dropDuplicates(["event_time", "event_type", "product_id", "user_id"]).count() }.')

    ec.select(*[
        (
            F.count(F.when((F.isnan(c) | F.col(c).isNull()), c)) if t not in ("timestamp", "date")
            else F.count(F.when(F.col(c).isNull(), c))
        ).alias(c)
        for c, t in ec.dtypes 
    ]).show()

    # Contagem de NaN na coluna category_code
    print(f'Ecommerce - number of rows is {ec.count()}; after isnan() applied on column category_code would be {ec.filter(~isnan(col("category_code"))).count()}.')

    ec.groupBy("category_id", "category_code") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)

    # Não há categorias com mais de um código
    ec.groupBy("category_id") \
    .agg(countDistinct("category_code").alias("num_codes")) \
    .filter("num_codes > 1") \
    .orderBy("num_codes", ascending=False) \
    .show(truncate=False)

    # Não há produtos iguais com categorias diferentes
    ec.groupBy("product_id") \
    .agg(countDistinct("category_id").alias("cat_codes")) \
    .filter("cat_codes > 1") \
    .orderBy("cat_codes", ascending=False) \
    .show(truncate=False)

    # Exemplo de atividade de um utilizador agregada por evento
    ec.groupBy("product_id", "user_id", "event_type", "user_session") \
        .count() \
        .filter(col("user_id") == 557746614) \
        .orderBy("count", ascending=False) \
        .show(truncate=False)

    # Exemplo de atividade de um utilizador
    print(ec.filter(col("user_id") == 557746614) \
    .orderBy("event_time") \
    .select("event_time", "product_id", "user_id", "event_type", "user_session") \
    .toPandas())


    # Pie chart para eventos
    event_counts = ec.groupBy("event_type").agg(count("*").alias("count"))
    event_counts = event_counts.toPandas()

    pie = px.pie(
        event_counts,
        names='event_type',
        values='count',
        title='Event Type Distribution',
    )

    pie.update_layout(
        legend_title_text='Event Type'
    )

    pie.show()


    brand_counts = ec.filter(~isnan(col("brand"))) \
                    .groupBy("brand") \
                    .agg(count("product_id").alias("count"))


    top_10_brands = brand_counts.orderBy(col("count").desc()).limit(10)

    top_10_brands = top_10_brands.toPandas()

    fig = px.bar(
        top_10_brands,
        x='brand',
        y='count',
        title='Top 10 Brands',
        labels={'brand': 'Brand Name', 'count': 'Number of Occurrences'}
    )

    fig.show()


    #Split da categoria e sub categoria
    div_cat = ec \
        .withColumn("main_category", split("category_code", "\.")[0]) \
        .withColumn("sub_category", split("category_code", "\.")[1])

    #Pie Chart Categorias Principais
    main_category_counts = div_cat.select("*") \
                            .filter(~isnan(col("category_code"))) \
                            .groupBy("main_category") \
                            .agg(count("product_id").alias("total_count"))

    main_category_counts = main_category_counts.orderBy(col("total_count").desc()).toPandas()

    print('Number of main-categories: ', len(main_category_counts))

    fig = px.pie(
        data_frame=main_category_counts,
        names='main_category',
        values='total_count',
        hole=0.3
    )

    fig.update_layout({
        "title": {"text": "Main category distribution", "x": 0.50}
    })

    fig.show()


    # Bar plot 20 maiores categorias
    sub_category_counts = div_cat.filter(~isnan(col("category_code"))) \
        .groupBy("sub_category") \
        .agg(count("product_id").alias("total_count")) \
        .orderBy(col("total_count").desc())

    top_20_category = sub_category_counts.limit(20).toPandas()

    fig = px.bar(top_20_category, x='sub_category', y='total_count', title='Top 20 Sub-Categories')
    fig.show()


    # Produtos com valor 0
    zero_price = ec.filter(col("price") == 0)

    zero_price.show()


    # Distribuição dos preços dos produtos (corrigido para não imcluir zeros)
    prices_0_table = ec.select("price")

    prices_0_table = prices_0_table.withColumn(
        "price_category",
        F.try_divide(F.col("price"), F.col("price"))
    ).fillna(0, subset=["price_category"]).withColumn("price_category", F.col("price_category").cast(StringType()))
    max_price = prices_0_table.select(F.max(prices_0_table.price)).first()[0]
    splits = np.append((np.linspace(0, 1, num=99) * max_price).astype(int), float("inf"))
    bucket = Bucketizer(inputCol="price", outputCol="price_bucket", splits=splits, handleInvalid="keep"
                    )
    prices_0_table = bucket.transform(prices_0_table)
    prices_grouped = prices_0_table.groupBy(["price_category", "price_bucket"]).count().toPandas().sort_values("price_bucket").reset_index(drop=True)
    prices_grouped["price_bucket_real"] = prices_grouped["price_bucket"].apply(lambda x: splits[int(x)])

    # Create the stacked bar chart
    fig = px.bar(
        prices_grouped, 
        x='price_bucket_real', 
        y="count",
        title='Price Distribution',
        color='price_category',  
        barmode='stack', 
    )

    # Customize layout for axis titles
    fig.update_layout(
        xaxis_title="Price",
        yaxis_title="Number of Occurrences",
        legend_title="Price Category"
    )

    fig.show()


    # Histograma de tipo de evento por preço de produto
    ec = bucket.transform(ec)
    prices_action = ec.groupBy(["price_bucket", "event_type"]).count().toPandas()
    prices_action["price_bucket_real"] = prices_action["price_bucket"].apply(lambda x: splits[int(x)])

    fig = px.bar(
        prices_action,
        x='price_bucket_real',
        y="count",
        title='Price Distribution by Event Type',
        color='event_type',  
        barmode='stack'
    )

    fig.update_layout(
        xaxis_title="Price",
        yaxis_title="Number of Occurrences",
        legend_title="Event Type"
    )

    fig.show()


    # Histograma de tipo de evento por preço de produto

    # prices_action_rel = ec.filter(col("event_type") != "view").select("price", "event_type").toPandas()
    prices_action_rel = prices_action[prices_action["event_type"] != "view"]
    fig = px.histogram(
        prices_action_rel,
        x='price_bucket_real',
        y="count",
        title='Price Distribution by Event Type (Excluding views)',
        color='event_type',  
        barmode='stack'
    )

    fig.update_layout(
        xaxis_title="Price",
        yaxis_title="Number of Occurrences",
        legend_title="Event Type"
    )

    fig.show()