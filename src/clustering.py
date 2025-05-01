# Importações Básicas
import pyspark
import pandas as pd
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.functions import split, isnan, col, isnull, avg, count, round
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)
from pyspark.ml.feature import VectorAssembler, MaxAbsScaler
from pyspark.ml.clustering import KMeans, GaussianMixture, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt 


def clustering(spark: SparkSession, data_dir: str, parquet_dir: str, sample_size: float, k: int = 4, chosen_model: int = 0):
    # # Leitura de dados
    # data_dir = '../data/raw/'
    # file_ec = data_dir + 'user_df_full.parquet'

    # Carrega as primeiras 10.000 linhas
    ec_user = spark.read.parquet(parquet_dir)

    ec_user = ec_user.repartition(36, "total_views")

    ec_user.show()

    feature_cols = ec_user.columns
    feature_cols.pop(0)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_prescale")
    client_features = assembler.transform(ec_user)

    scaler = MaxAbsScaler(inputCol="features_prescale", outputCol="features")
    client_features = scaler.fit(client_features).transform(client_features).repartition(36, "total_views")

    client_features.show()

    silhouette_score=dict()
    models = [KMeans(), GaussianMixture(), BisectingKMeans()]
    
    evaluator = ClusteringEvaluator(predictionCol='prediction', 
                                    featuresCol='features',  
                                    metricName='silhouette',  
                                    distanceMeasure='squaredEuclidean') 

    # for model in models:
    #     print(f"Now iterating over {model}")
    #     silhouette_score[str(model)] = []
    #     model = model.setFeaturesCol('features').setSeed(42).setMaxIter(10)
    #     for i in range(2,10): 
    #         model = model.setK(i)
    #         fitted_model=model.fit(client_features) 
    #         predictions=fitted_model.transform(client_features) 
    #         score=evaluator.evaluate(predictions) 
    #         silhouette_score[str(model)].append(score) 
    #         print('Silhouette Score for k =',i,'is',score)
    #     print()
    
    # for model in models:
    #     plt.plot(range(2,10),silhouette_score[str(model)]) 
    #     plt.xlabel('k') 
    #     plt.ylabel('silhouette score') 
    #     plt.title(f'Silhouette Score for {model}') 
    #     plt.show()

    # print(models)

    chosen_model = models[chosen_model]
    # optimal_k = silhouette_score[str(chosen_model)].index(max(silhouette_score[str(chosen_model)])) + 2
    optimal_k = k

    # Trains a k-means model.
    model = chosen_model.setSeed(42).setFeaturesCol("features").setK(optimal_k)
    fitted_model = model.fit(client_features)
    print(f"Cluster Sizes: {fitted_model.summary.clusterSizes}")

    # Make predictions
    predictions = fitted_model.transform(client_features)

    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # Shows the result.
    centers = fitted_model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    predictions.groupBy("prediction").count().show()

    cluster_profiles = predictions.groupBy("prediction").mean()
    for c in cluster_profiles.columns:
        cluster_profiles = cluster_profiles.withColumn(c, F.round(c, 2))
    cluster_profiles.show()

    # write in a parquet file
    predictions.drop("features_prescale", "features").write.parquet(data_dir + f"processed/user_cluster_{sample_size}.parquet", mode="overwrite")