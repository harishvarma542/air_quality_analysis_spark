from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, lead, when, row_number, udf, to_date, hour
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AirQuality_SQL_Analysis_CSV") \
    .getOrCreate()

# Input and output paths
input_csv = "/workspaces/air_quality_analysis_spark/output_task2/enhanced_data/task2_enhanced_data.csv"
output_folder = "/workspaces/air_quality_analysis_spark/output_task3"
os.makedirs(output_folder, exist_ok=True)

# Load the CSV (infer schema automatically)
df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)
print("✅ Loaded CSV Data Successfully!")

# 🛠 Extract date and hour from timestamp
df = df.withColumn("date", to_date("timestamp")) \
       .withColumn("hour", hour("timestamp"))

# Create a Temp View
df.createOrReplaceTempView("air_quality_data")
print("✅ Created Temp View: air_quality_data")

# 1️⃣ Regions with Highest Avg PM2.5
query1 = spark.sql("""
    SELECT region, AVG(pm25) AS avg_pm25
    FROM air_quality_data
    GROUP BY region
    ORDER BY avg_pm25 DESC
""")
query1.show()
query1.write.mode("overwrite").csv(os.path.join(output_folder, "highest_avg_pm25"), header=True)

# 2️⃣ Peak Pollution Hours
query2 = spark.sql("""
    SELECT date, hour, region, pm25
    FROM air_quality_data
    ORDER BY pm25 DESC
    LIMIT 10
""")
query2.show()
query2.write.mode("overwrite").csv(os.path.join(output_folder, "peak_pollution_intervals"), header=True)

# 3️⃣ Trend Analysis: Rolling Increase Detection
window_spec = Window.partitionBy("region").orderBy("timestamp")

trend_df = df.withColumn("prev_pm25", lag("pm25", 1).over(window_spec)) \
             .withColumn("pm25_increase", col("pm25") - col("prev_pm25"))

trend_df.createOrReplaceTempView("trend_analysis")

query3 = spark.sql("""
    SELECT region, timestamp, pm25, prev_pm25, pm25_increase
    FROM trend_analysis
    WHERE pm25_increase > 0
    ORDER BY region, timestamp
""")
query3.show()
query3.write.mode("overwrite").csv(os.path.join(output_folder, "pm25_trend_increase"), header=True)

# 4️⃣ Air Quality Index (AQI) Classification
def classify_aqi(pm25_value):
    if pm25_value <= 50:
        return "Good"
    elif pm25_value <= 100:
        return "Moderate"
    else:
        return "Unhealthy"

udf_aqi = udf(classify_aqi, StringType())

classified_df = df.withColumn("AQI_Category", udf_aqi(col("pm25")))

classified_df.createOrReplaceTempView("aqi_classification")

query4 = spark.sql("""
    SELECT region, timestamp, pm25, AQI_Category
    FROM aqi_classification
""")
query4.show()
query4.write.mode("overwrite").csv(os.path.join(output_folder, "aqi_classification"), header=True)

print("🎯 Task 3 (CSV Version) - COMPLETED Successfully!")
print(f"All outputs saved inside ➡ {output_folder}")

# Stop Spark Session
spark.stop()