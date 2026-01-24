from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Kafka-to-Delta-Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")



from pyspark.sql.types import *

order_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_time", StringType()),

    StructField("order_id", StringType()),
    StructField("user_id", StringType()),

    StructField("city", StringType()),
    StructField("zone", StringType()),

    StructField("order_amount", DoubleType()),
    StructField("item_count", IntegerType()),
    StructField("item_ids", ArrayType(StringType())),

    StructField("payment_method", StringType()),
    StructField("platform", StringType())
])


from pyspark.sql.functions import *

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
    .select(from_json(col("json_str"), order_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time"))
    .withColumn("ingest_time", to_timestamp("ingest_time"))
)


query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "hdfs://localhost:9000/inputcheckpoints/bronze/orders")
    .trigger(processingTime="0.4 seconds")
    .start("hdfs://localhost:9000/lakehouse/bronze/orders")
)

query.awaitTermination()
