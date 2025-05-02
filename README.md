# Air Quality Monitoring & Forecasting Project

## Overview
This project models a near-real-time air quality monitoring workflow using Apache Spark. It captures and processes sensor data—specifically PM2.5, temperature, and humidity—through a structured pipeline that spans data ingestion, cleansing, transformation, predictive modeling, and interactive visualization.

Core Components
- Real-time data ingestion over TCP
- Outlier detection and data preprocessing
- Analytical queries via Spark SQL
- PM2.5 prediction using Spark MLlib
- Interactive visual dashboards and reports

---

##  Technology Stack
- Languages & Libraries: Python, PySpark
- Frameworks: Apache Spark (Structured Streaming, SQL, MLlib)
- Data Tools: Pandas, NumPy
- Visualization: Plotly, Seaborn, Matplotlib, Kaleido

---

##  Pipeline Sections

###  Section 1: Data Ingestion and Initial Pre-Processing
- Streams historical sensor data (PM2.5, temperature, humidity) through a TCP socket.
- Leverages Spark Structured Streaming with watermarking for late data handling.
- Consolidates metrics into single timestamp-region rows.
- Output: Raw batches and a unified CSV file.

**Script**: `section1.py`

---

###  Section 2:  Data Aggregations, Transformations & Trend Analysis
- Cleans data by addressing missing values and outliers.
- Adds new features: rolling averages, lag values.
- Groups data hourly and daily by timestamp and region.
- Output: Cleaned data in output_task2/cleaned_data/

**Script**: `section2.py`

---

###  Section 3: Spark SQL Exploration & Correlation Analysis
- Uses Spark SQL to identify trends and derive insights.
- Employs window functions for moving average calculations.
- Implements a UDF to classify AQI categories.
- Output: Resulting CSVs in output_task3/sql_outputs/

**Script**: `section3.py`

---

###  Section 4: Spark MLlib
Builds and tunes a Random Forest Regressor for PM2.5 prediction.
Splits data into training and testing subsets.
Evaluates model performance using RMSE and R².
Output: Predictions saved in output_task4/model_predictions/
Optional: Model persistence
- Builds and tunes a Random Forest Regressor for PM2.5 prediction.
- Splits data into training and testing subsets.
- Evaluates model performance using RMSE and R².
- Output: Predictions saved in output_task4/model_predictions/

**Script**: `section4.py`  

---

###  Section 5: Pipeline Integration & Dashboard Visualization
- Merges all steps into a single executable pipeline.
- Generates interactive dashboards including:
  - Line plots (actual vs. predicted PM2.5)
  - Scatter plots (spike detection)
  - AQI category pie charts
  - Correlation heatmaps
- Outputs both HTML and PNG formats.
- Output: output_task5/final_output/

**Script**: `section5.py`  

---

##  Folder Structure
```
air_quality_project/
├── section1_ingestion.py
├── section2_transformations.py
├── section3_sql_queries.py
├── section4_Spark_MLlib.py
├── section5_pipeline_dashboard.py
├── output_task2/cleaned_data/
├── output_task3/sql_outputs/
├── output_task4/model_predictions/
├── output_task5/final_output/
└── README.md
```

---

## Deliverables
A complete, end-to-end system featuring:
- Real-time ingestion
- Cleaned and transformed data
- Analytical SQL insights
- Machine learning forecasts
- Interactive visual dashboards

> Ready for demonstration, stakeholder reports, and real-time monitoring.

---

##  How to Run

1. Install dependencies:
```bash
pip install pandas numpy plotly kaleido seaborn matplotlib
```

2. Start TCP ingestion:
```bash
python tcp_log_file_streaming_server.py
```

3. Run each section:
```bash
spark-submit section1.py
spark-submit section2.py
spark-submit section3.py
spark-submit section4.py
spark-submit section5.py
```

---

