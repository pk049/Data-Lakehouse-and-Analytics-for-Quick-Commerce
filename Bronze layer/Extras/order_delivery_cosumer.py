# ===================================================
# Delivery Riders fetches this from kafka Queue
# ===================================================


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    expr
)
from pyspark.sql.types import *

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("order_delivery_processor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Schema (same as processed topic)
# ============================================================
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("city", StringType()),
    StructField("zone", StringType()),
    StructField("items", IntegerType()),
    StructField("items_count", IntegerType()),
    StructField("order_amount", IntegerType()),
    StructField("payment_method", StringType()),
    StructField("platform", StringType()),
    StructField("ingest_time", TimestampType()),
    StructField("order_processed_time", TimestampType())
])

# ============================================================
# Read from Kafka (Processed Orders)
# ============================================================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders_processed")
    .option("startingOffsets", "latest")
    .load()
)

# ============================================================
# Parse + Transform
# ============================================================
delivered_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("delivery_ingest_time", current_timestamp())
    # delivered_time = processed_time + random 10–15 minutes
    .withColumn(
        "delivered_time",
        expr(
            "order_processed_time + (600 + cast(rand() * 300 as int)) * INTERVAL 1 SECOND"
        )
    )
)

# ============================================================
# Write to HDFS (Delta Lake)
# ============================================================
query = (
    delivered_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "path",
        "hdfs://localhost:9000/user/pratik/project/orders/orders_bronze/delivered/"
    )
    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/user/pratik/project/checkpoints/orders_bronze/delivered/"
    )
    .trigger(processingTime="2 seconds")
    .start()
)

# ============================================================
# Await Termination
# ============================================================
query.awaitTermination()
