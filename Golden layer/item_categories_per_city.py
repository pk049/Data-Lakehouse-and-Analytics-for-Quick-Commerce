from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("Silver-To-Hive-Compatible")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_df = (
    spark.read
    .format("delta")
    .load("hdfs://localhost:9000/user/pratik/project/orders/orders_silver/")
)

item_struct = StructType([
    StructField("item_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("line_total", DoubleType())
])


parsed_df = silver_df.select(
    col("event_id"),
    col("event_type"),
    col("event_time").cast("string"),
    col("order_id"),
    col("user_id"),
    col("city"),
    col("zone"),

    expr(f"""
        transform(items, x -> from_json(x, '{item_struct.simpleString()}'))
    """).alias("items_parsed"),

    col("item_count"),
    col("order_amount").cast("int"),
    col("payment_method"),
    col("platform")
)



exploded_df = (
    parsed_df
    .select(
        col("order_id"),
        col("city"),
        explode("items_parsed").alias("item")
    )
    .select(
        col("order_id"),
        col("city"),
        col("item.item_id"),
        col("item.quantity"),
        col("item.line_total")
    )
)

gold_df = (
    exploded_df
    .groupBy("city")
    .agg(count("*").alias("count_per_city"))
)


gold_path = "hdfs://localhost:9000/user/pratik/project/orders/orders_gold/item_category_per_city"

(
    gold_df
    .write
    .mode("overwrite")          # or "append" for daily runs
    .format("parquet")
    .save(gold_path)
)

print("✅ Gold table written to:", gold_path)


# hdfs://localhost:9000/user/pratik/project/orders/orders_gold/
