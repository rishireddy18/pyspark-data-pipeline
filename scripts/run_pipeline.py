from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Customer Pipeline") \
    .getOrCreate()

# Read input
df = spark.read.json("data/input/customers.json")

# Clean nulls
df_clean = df.filter(col("name").isNotNull())

# Write as partitioned Parquet
df_clean.write.mode("overwrite") \
    .partitionBy("country") \
    .parquet("data/output/customers")

spark.stop()
