# SECTION 5: Unified Pipeline + Dashboard Visualization (with PNG export)

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import plotly.io as pio

# Use kaleido for image export
pio.kaleido.scope.default_format = "png"

# Ensure output folders exist
os.makedirs("output_task5/final_output", exist_ok=True)

# --- SECTION 1: Load Raw Ingested Data ---
raw_data_path = "output/combined_data_singlefile/"
raw_df = pd.concat([
    pd.read_csv(os.path.join(raw_data_path, f))
    for f in os.listdir(raw_data_path) if f.endswith(".csv")
], ignore_index=True)
raw_df.to_csv("output_task5/final_output/section1_combined_raw.csv", index=False)

# --- SECTION 2: Load Cleaned Transformed Data ---
cleaned_csv = "output_task2/cleaned_data/task2_cleaned_data.csv"
cleaned_df = pd.read_csv(cleaned_csv)
cleaned_df.to_csv("output_task5/final_output/section2_transformed_clean.csv", index=False)

# --- SECTION 3: SQL Output (Simulated path) ---
sql_output_path = "output_task3/sql_outputs/"
os.makedirs(sql_output_path, exist_ok=True)
# If you have SQL outputs, load them here.

# --- SECTION 4: Load ML Predictions ---
predictions_csv = "output_task4/model_predictions/"
pred_df = pd.concat([
    pd.read_csv(os.path.join(predictions_csv, f))
    for f in os.listdir(predictions_csv) if f.endswith(".csv")
], ignore_index=True)

# --- Merge Cleaned Data + Predictions ---
full_df = cleaned_df.copy()
full_df = full_df.iloc[:len(pred_df)]  # align
full_df["predicted_pm25"] = pred_df["prediction"]
full_df.to_csv("output_task5/final_output/final_data_with_predictions.csv", index=False)

# ---------------- DASHBOARDS ---------------- #

# 1. Time-Series Chart: Actual vs Predicted PM2.5
fig1 = go.Figure()
fig1.add_trace(go.Scatter(y=full_df["pm25"], mode="lines", name="Actual PM2.5"))
fig1.add_trace(go.Scatter(y=full_df["predicted_pm25"], mode="lines", name="Predicted PM2.5"))
fig1.update_layout(title="Actual vs Predicted PM2.5 Over Time", xaxis_title="Index", yaxis_title="PM2.5")
fig1.write_html("output_task5/final_output/actual_vs_predicted.html")
fig1.write_image("output_task5/final_output/actual_vs_predicted.png")

# 2. Spike Timeline: PM2.5 > 100
spike_df = full_df[full_df["pm25"] > 100]
fig2 = px.scatter(spike_df, y="pm25", title="PM2.5 Spikes > 100", labels={"index": "Time"})
fig2.write_html("output_task5/final_output/pm25_spikes.html")
fig2.write_image("output_task5/final_output/pm25_spikes.png")

# 3. AQI Classification Breakdown (Pie Chart)
conditions = [
    (full_df["pm25"] <= 50),
    (full_df["pm25"] > 50) & (full_df["pm25"] <= 100),
    (full_df["pm25"] > 100)
]
labels = ["Good", "Moderate", "Unhealthy"]
full_df["AQI_Category"] = np.select(conditions, labels)
fig3 = px.pie(full_df, names="AQI_Category", title="AQI Classification Breakdown")
fig3.write_html("output_task5/final_output/aqi_breakdown.html")
fig3.write_image("output_task5/final_output/aqi_breakdown.png")

# 4. Correlation Heatmap
corr = full_df[["pm25", "temperature", "humidity"]].corr()
plt.figure(figsize=(6, 4))
sns.heatmap(corr, annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap")
plt.tight_layout()
plt.savefig("output_task5/final_output/correlation_heatmap.png")

# ✅ Done
print("\n✅ Section 5 Completed: All outputs saved in 'output_task5/final_output/' folder.")