from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = (
    SparkSession.builder
    .appName("order_consumer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

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

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order_placed_bronze") \
    .option("startingOffsets", "earliest") \
    .load()


parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn(
        "event_time",
        expr("current_timestamp() + make_interval(0, 0, 0, 0, 0, cast(rand() * 60 as int))")
    )
)

query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", "hdfs://localhost:9000/user/pratik/project/orders/orders_bronze/")
    .option("checkpointLocation", "hdfs://localhost:9000/user/pratik/project/checkpoints/orders_bronze/")
    .trigger(processingTime="2 seconds")
    .start()
)

query.awaitTermination()

spark.stop()


# can spark read kafka offset
# check kafka key from topic

