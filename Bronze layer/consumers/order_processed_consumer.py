# =================================================================
# Stores fetches this from kafka Queue and send to delivery queue
# =================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    expr
)
from pyspark.sql.types import *

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("order_processor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Schema
# ============================================================
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("city", StringType()),
    StructField("zone", StringType()),
    StructField("items", ArrayType(elementType=StringType())),
    StructField("item_count", IntegerType()),
    StructField("order_amount", IntegerType()),
    StructField("payment_method", StringType()),
    StructField("platform", StringType())
])

# ============================================================
# Read from Kafka (Bronze)
# ============================================================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "order_placed_bronze")
    .option("startingOffsets", "latest")
    .load()
)

# ============================================================
# Parse + Transform
# ============================================================
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time"))
    # random processing delay between 60–120 seconds PER ROW
    .withColumn(
        "order_processed_time",
        expr(
            "event_time + (60 + cast(rand() * 60 as int)) * INTERVAL 1 SECOND"
        )
    )
)

# ============================================================
# Write to Delta Lake
# ============================================================
delta_query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "path",
        "hdfs://localhost:9000/user/pratik/project/orders/orders_bronze/processed/"
    )
    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/user/pratik/project/checkpoints/orders_bronze/processed/"
    )
    .trigger(processingTime="2 seconds")
    .start()
)

# ============================================================
# Prepare Kafka Output (Processed Orders)
# ============================================================
# kafka_out_df = (
#     parsed_df
#     .selectExpr(
#         "CAST(order_id AS STRING) AS key",
#         "to_json(struct(*)) AS value"
#     )
# )

# ============================================================
# Write to Kafka (Processed Topic)
# ============================================================
# kafka_query = (
#     kafka_out_df.writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "localhost:9092")
#     .option("topic", "delivery_queue")
#     .option(
#         "checkpointLocation",
#         "hdfs://localhost:9000/user/pratik/project/checkpoints/orders_sent/processed/"
#     )
#     .outputMode("append")
#     .start()
# )

# ============================================================
# Await Termination
# ============================================================
spark.streams.awaitAnyTermination()
