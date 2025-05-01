from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, rand, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import time
import os

# Step 1: Create a Spark Session
spark = SparkSession.builder \
    .appName("AirQuality_Ingestion_And_Merge") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Define the expected schema
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("region", StringType(), True),
    StructField("pm25", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True)
])

# Step 3: Define output folders
batch_output_path = "/workspaces/air_quality_analysis_spark/output/batch/"
merged_output_path = "/workspaces/air_quality_analysis_spark/output/merged_data/"
checkpoint_path = "/workspaces/air_quality_analysis_spark/output/checkpoint/"

# Ensure output folders exist
os.makedirs(batch_output_path, exist_ok=True)
os.makedirs(checkpoint_path, exist_ok=True)
os.makedirs(merged_output_path, exist_ok=True)

# Step 4: Read the raw TCP Stream
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 5: Parse each line into structured columns
parsed_stream = raw_stream.select(
    split(raw_stream["value"], ",").alias("cols")
).select(
    col("cols").getItem(3).cast(TimestampType()).alias("timestamp"),
    col("cols").getItem(2).alias("region"),
    col("cols").getItem(8).cast(FloatType()).alias("pm25"),
    spark_round(rand() * 15 + 20, 2).alias("temperature"),
    spark_round(rand() * 40 + 30, 2).alias("humidity")
)

# Step 6: Apply watermarking
watermarked_stream = parsed_stream.withWatermark("timestamp", "10 minutes")

# Step 7: Write the parsed stream to batch CSV files
query = watermarked_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", batch_output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("header", "true") \
    .start()

print("Streaming started. Batches are being saved. Press Ctrl+C to stop.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming manually stopped. Proceeding to merge batches.")

    # Step 8: After streaming stops, load all batch files
    merged_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(batch_output_path)

    print("Loaded all batch CSV files.")

    # Step 9: Merge and save into a single clean CSV
    merged_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(merged_output_path)

    print(f"Merged single CSV saved at: {merged_output_path}")
