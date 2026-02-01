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

golden_df = (
    spark.read
    .format("parquet")
    .load("hdfs://localhost:9000/user/pratik/project/orders/orders_gold/5min_window/")
)


golden_df.show(truncate=False)