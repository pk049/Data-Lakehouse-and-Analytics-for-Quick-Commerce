from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp,explode,from_json,count,asc,desc,avg,window,count
from pyspark.sql.types import *

# Spark is adding UTC()-------->IST(5.30)


item_schema = StructType([
    StructField("item_id", StringType()),
    StructField("quantity", IntegerType()),
])


# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("read_processed_orders")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
# ============================================================
# Read Delta Table (Batch)
# ============================================================
df = (
     spark.read
    .format("delta")
    .load("hdfs://localhost:9000/user/pratik/project/orders/orders_silver/")
)

windowed_df = (
    df
    .groupBy(
        window(col("event_time"), "5 minutes")
    )
    .agg(count("*").alias("order_count"))
    .orderBy("window")
)

series_df = windowed_df.select(
    col("window.start").alias("time"),
    col("order_count")
).orderBy("time")


series_df.write \
    .format("delta") \
    .mode("append") \
    .save("hdfs://localhost:9000/user/pratik/project/orders/rnn_data")


windowed_df.show(n=200,truncate=False)

