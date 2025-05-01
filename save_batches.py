from pyspark.sql import SparkSession

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("AirQuality_Combine_Batches_SingleFile") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Load All Saved Batch Files
combined_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/workspaces/air_quality_analysis_spark/output/batch/")

print("Loaded and Combined All Batches")
combined_df.show(5)

# Step 3: Save the Combined Data as a SINGLE CSV File
combined_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/workspaces/air_quality_analysis_spark/output/combined_data_singlefile/")

print("Combined Data Saved as a Single File at /output/combined_data_singlefile/")