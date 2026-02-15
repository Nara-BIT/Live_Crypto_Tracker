from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("CryptoAnalytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Define Schema (Must match the Producer data)
schema = StructType([
    StructField("currency", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True) # Coming in as a number
])

# 3. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "crypto-stream") \
    .load()

# 4. Parse JSON & Add a proper Timestamp column
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Add a "processing_time" column so Spark can do time-based math
# In a real app, we would convert the 'timestamp' field, but this is simpler for now.
formatted_df = parsed_df.withColumn("processing_time", current_timestamp())

# 5. The Analysis: Calculate 1-minute Average Price
# "Group by Currency and a 1-minute Time Window"
windowed_counts = formatted_df \
    .groupBy(
        window(col("processing_time"), "1 minute"), 
        col("currency")
    ) \
    .agg(avg("price").alias("avg_price")) \
    .select("window.start", "window.end", "currency", "avg_price")

# 6. Output to Console
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()