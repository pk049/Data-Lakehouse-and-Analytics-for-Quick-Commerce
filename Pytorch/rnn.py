# ============================
# STEP 0: Import libraries
# ============================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import torch
import torch.nn as nn

# ============================
# STEP 1: Load Delta data in Spark
# ============================

spark = (
    SparkSession.builder
    .appName("DemandForecastRNN")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ----------------------------
# Load Delta table (windowed 5-min order counts)
# ----------------------------
delta_path = "hdfs://localhost:9000/user/pratik/project/orders/rnn_data/"
spark_df = spark.read.format("delta").load(delta_path)

# ============================
# STEP 2: Convert Spark DataFrame to Pandas + NumPy series
# ============================

def spark_to_series(spark_df):
    """
    Convert a Spark DataFrame of 5-minute windowed order counts
    to a NumPy 1D array for PyTorch.

    Expected columns:
      - time  (timestamp)
      - order_count (int)
    """
    pdf = (
        spark_df.orderBy("time")
        .select("order_count")
        .toPandas()
    )
    series = pdf["order_count"].values
    return series

series = spark_to_series(spark_df)
print("Series loaded for RNN:", series)

# ============================
# STEP 3: Prepare sliding windows
# ============================

def make_sequences(series, lookback=6, horizon=3):
    """
    Create sliding sequences for RNN.

    lookback=6  → last 30 minutes (6 × 5 min)
    horizon=3   → predict next 15 minutes
    """
    X, y = [], []
    for i in range(len(series) - lookback - horizon + 1):
        X.append(series[i:i+lookback])
        y.append(series[i+lookback:i+lookback+horizon])
    return np.array(X), np.array(y)

lookback = 6   # 30 minutes input
horizon = 3    # next 15 minutes prediction

X, y = make_sequences(series, lookback, horizon)

# Convert to PyTorch tensors
X_tensor = torch.tensor(X, dtype=torch.float32).unsqueeze(-1)  # (samples, time_steps, 1)
y_tensor = torch.tensor(y, dtype=torch.float32)                # (samples, horizon)

print("X_tensor shape:", X_tensor.shape)
print("y_tensor shape:", y_tensor.shape)

# ============================
# STEP 4: Define LSTM RNN Model
# ============================

class DemandRNN(nn.Module):
    """
    LSTM-based model for demand forecasting.
    """
    def __init__(self, input_size=1, hidden_size=64, output_size=horizon):
        super(DemandRNN, self).__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            batch_first=True
        )
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        out, _ = self.lstm(x)  
        out = out[:, -1, :]     # last time step
        out = self.fc(out)
        return out

# ============================
# STEP 5: Train the RNN model
# ============================

model = DemandRNN()
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

epochs = 50
for epoch in range(epochs):
    optimizer.zero_grad()
    y_pred = model(X_tensor)
    loss = criterion(y_pred, y_tensor)
    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item():.4f}")

# ============================
# STEP 6: Predict next windows
# ============================

def predict_next_windows(model, series, lookback=6):
    """
    Predict next 'horizon' windows using latest past 'lookback' windows.
    """
    latest = series[-lookback:]  # last 30 minutes
    input_tensor = torch.tensor(latest, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)

    with torch.no_grad():
        prediction = model(input_tensor).numpy().flatten()

    return prediction

# ----------------------------
# Generate prediction
# ----------------------------

prediction = predict_next_windows(model, series, lookback)

# Last 15 minutes actual (3 windows)
last_15_min_actual = series[-horizon:]  # last 3 windows

print("\n================= DEMAND FORECAST REPORT =================")
print(f"Last 30 min input to model (6 windows): {series[-lookback:]}")
print(f"Last 15 min actual (3 windows):         {last_15_min_actual}")
print("-----------------------------------------------------------")
print(f"Predicted next 15 min (3 windows):      {prediction}")
print("===========================================================\n")

print("NOTES:")
print("- Each window = 5 minutes")
print("- Last 15 min actual = true recent demand")
print("- Predicted next 15 min = expected demand")
print("- Useful for delivery rider allocation, inventory, staffing, etc.")
