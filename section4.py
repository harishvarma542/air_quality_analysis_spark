from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("AirQuality_Section4_Regression") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Load cleaned Task 2 data
input_path = "output_task2/cleaned_data/task2_cleaned_data.csv"
output_path = "output_task4/model_predictions"
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Step 3: Select relevant features and target
feature_cols = ["temperature", "humidity"]
target_col = "pm25"

# Drop rows with nulls in selected columns
df_clean = df.select(target_col, *feature_cols).dropna()

# Step 4: Assemble features into a vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(df_clean).select("features", target_col)

# Step 5: Split into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Step 6: Train Random Forest Regressor
rf = RandomForestRegressor(featuresCol="features", labelCol=target_col)

# Step 7: Build Param Grid for Tuning
paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10]) \
    .addGrid(rf.numTrees, [20, 50]) \
    .build()

# Step 8: Setup Evaluator
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")

# Step 9: Setup CrossValidator
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

# Step 10: Fit model
cv_model = cv.fit(train_data)

# Step 11: Evaluate on test data
predictions = cv_model.transform(test_data)
rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2").evaluate(predictions)

print("\n✅ Best Model Performance:")
print(f"RMSE: {rmse:.2f}")
print(f"R²: {r2:.2f}")

# Step 12: Save predictions to output folder (CSV-compatible)
predictions.selectExpr(
    f"CAST({target_col} AS DOUBLE) as {target_col}",
    "CAST(prediction AS DOUBLE) as prediction"
).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

print(f"✅ Predictions saved to: {output_path}")