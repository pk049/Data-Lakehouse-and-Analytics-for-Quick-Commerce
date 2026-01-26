from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("bronze_to_silver_stream")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


bronze_df = (
    spark.readStream
    .format("delta")
    .load("hdfs://localhost:9000/user/pratik/project/orders/orders_bronze/")
)

validated_df = (
    bronze_df
    .withColumn(
        "items_parsed",
        expr(
            "transform(items, x -> from_json(x, 'item_id STRING, quantity INT, line_total DOUBLE'))"
        )
    )
    .withColumn("check_count", size(col("items_parsed")))
    .withColumn(
        "has_negative_qty",
        expr("exists(items_parsed, x -> x.quantity < 0)")
    )
    .withColumn(
        "missing_payment_method",
        col("payment_method").isNull()
    )
)

silver_df = validated_df.filter(
    (col("item_count") == col("check_count")) &
    (~col("has_negative_qty")) &
    (~col("missing_payment_method"))
)


query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/user/pratik/project/checkpoints/orders_silver/"
    )
    .trigger(processingTime="2 seconds")
    .start(
        "hdfs://localhost:9000/user/pratik/project/orders/orders_silver/"
    )
)

query.awaitTermination()
