# Basic imports
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)
import os
import time


def datajoining(spark: SparkSession, data_dir: str, sample_sizes: list):
    # read csv files on data folder
    dataRaw_dir = data_dir + "raw/"

    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])

    # ec_total = spark.read.csv(data_dir, header=True, schema=schema)

    ec = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .csv(dataRaw_dir, header=True, schema=schema)

    print(f"Number of rows in total dataset file: {(count:=ec.count())}")

    # partition_per_columns = ["user_id", "user_session"]
    rows_per_output = 200000
    # ec = ec.repartition(int(count/rows_per_output)+1, *partition_per_columns)
    sample_durations = []
    for sample_size in sample_sizes:
        print(f"Starting {sample_size} sample")
        start = time.perf_counter()
        print(f"Number of rows in {sample_size} sample: {(sample_count:=int(count * sample_size))}")
        output_path = dataRaw_dir + f"ec_{sample_size}.parquet"
        if not os.path.exists(output_path):
            # ec.sample(fraction=sample_size, seed=777).coalesce(
            #         int(sample_count/rows_per_output)+1,  # make 1 more partition than needed (np.ceiling)
            #         # *partition_per_columns,  # does it speed this up?
            #         ) \
            #         .write.format("parquet").mode("overwrite").save(output_path)
            # ec.limit(sample_count).write.partitionedBy(F.partitioning.days("event_time")) \
            #     .format("parquet").mode("overwrite").save(output_path)
            #     #.repartition(
            #     #    int(sample_count/rows_per_output)+1,  # make 1 more partition than needed (np.ceiling)
            #     #    *partition_per_columns,)  # does it speed this up?
            # ec.limit(sample_count).write.format("parquet").mode("overwrite").save(output_path)
            #.coalesce(int(sample_count/rows_per_output)+1) \
            # .repartition(int(sample_count/rows_per_output)+1) \
            ec.sample(sample_size, seed=777) \
                .write.format("parquet").mode("overwrite").save(output_path)

        else:
            print("O diretório já existe. Não foi sobrescrito.")
        print(f"Took {(sample_duration:=time.perf_counter() - start):.2f} seconds for {sample_size} sample.", end="\n\n")
        sample_durations.append(sample_duration)
    return sample_durations