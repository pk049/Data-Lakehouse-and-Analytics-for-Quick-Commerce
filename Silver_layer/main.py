from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp,explode,from_json,count,asc,desc,avg
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
processed_df = (
     spark.read
    .format("delta")
    .load("hdfs://localhost:9000/user/pratik/project/orders/orders_bronze/processed/")
)

dimension_item=(
    spark.read\
    .format('delta')\
    .load('hdfs://localhost:9000/project/dimensions/item_schema/')
)


# ========================= Processed time ================================
time_df = (
    processed_df
    .select("event_id", "event_time", "order_processed_time")
    .withColumn(
        "processed_time_sec",
        unix_timestamp(col("order_processed_time")) -
        unix_timestamp(col("event_time"))
    ).agg(avg("processed_time_sec"))
)

time_df.show()

# ======================= items check df =================================
df = (
    processed_df
    .withColumn("item", explode("items"))   # item is STRING here
    .withColumn("item", from_json(col("item"), item_schema))  # convert to STRUCT
    .selectExpr("item.*").
    groupBy("item_id").
    agg(count("quantity").alias("count")).
    sort(desc("count"))
)

df.join(dimension_item, on="item_id", how="left").groupBy("category").agg(count("*").alias("count")).show()

# df.show(truncate=False)

# ============================================================
# Print Data
# ============================================================
# processed_df.show(truncate=False)
# processed_df.printSchema()
