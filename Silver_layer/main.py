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
    .load("hdfs://localhost:9000/user/pratik/project/orders/orders_silver/")
)

dimension_item=(
    spark.read\
    .format('delta')\
    .load('hdfs://localhost:9000/project/dimensions/item_schema/')
)


