from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("order_consumer")
    .getOrCreate()
)

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

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "order_placed_bronze")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time"))
    .withColumn("ingest_time", current_timestamp())
)

query = (
    parsed_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .trigger(processingTime="2 seconds")
    .start()
)

query.awaitTermination()
