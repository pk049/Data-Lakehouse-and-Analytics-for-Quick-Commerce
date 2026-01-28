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
delta_path = "hdfs://localhost:9000/user/pratik/project/orders/orders_gold/5min_window"
spark_df = spark.read.format("delta").load(delta_path)

# ============================
# STEP 2: Convert Spark DataFrame to Pandas
# ============================

def spark_to_series(spark_df):
    """
    Convert a Spark DataFrame of 5-minute windowed order counts
    to a NumPy 1D array for PyTorch.

    Parameters:
    -----------
    spark_df : pyspark.sql.DataFrame
        Spark DataFrame with columns:
            - time_start (timestamp)
            - order_count (int)

    Returns:
    --------
    series : np.ndarray
        1D NumPy array of order counts ordered by time.
    """
    pdf = (
        spark_df.orderBy("time")
        .select("order_count")
        .toPandas()
    )
    series = pdf["order_count"].values
    return series

series = spark_to_series(spark_df)
print("Series for RNN:", series)

# ============================
# STEP 3: Prepare sliding windows
# ============================

def make_sequences(series, lookback=6, horizon=3):
    """
    Create sliding input-output sequences for RNN training.

    Parameters:
    -----------
    series : np.ndarray
        1D array of order counts.
    lookback : int
        Number of past windows to use as input.
    horizon : int
        Number of future windows to predict.

    Returns:
    --------
    X : np.ndarray
        Input sequences of shape (samples, lookback)
    y : np.ndarray
        Output sequences of shape (samples, horizon)
    """
    X, y = [], []
    for i in range(len(series) - lookback - horizon + 1):
        X.append(series[i:i+lookback])
        y.append(series[i+lookback:i+lookback+horizon])
    return np.array(X), np.array(y)

lookback = 6  # 30 minutes (6 * 5-min windows)
horizon = 3   # predict next 15 minutes
X, y = make_sequences(series, lookback, horizon)

# Convert to PyTorch tensors
# ============================
X_tensor = torch.tensor(X, dtype=torch.float32).unsqueeze(-1)  # shape: (samples, lookback, features=1)
y_tensor = torch.tensor(y, dtype=torch.float32)                # shape: (samples, horizon)

print("X_tensor shape:", X_tensor.shape)
print("y_tensor shape:", y_tensor.shape)

# ============================
# STEP 4: Define PyTorch RNN
# ============================

class DemandRNN(nn.Module):
    """
    LSTM-based RNN for demand forecasting.

    Input:
        X_tensor: (batch_size, time_steps, features)
    Output:
        y_pred: (batch_size, horizon)
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
        out, _ = self.lstm(x)        # out: (batch, time_steps, hidden_size)
        out = out[:, -1, :]           # take the last time step
        out = self.fc(out)            # fully connected to horizon
        return out

# ============================
# STEP 5: Train the RNN
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
# STEP 6: Make a prediction
# ============================

def predict_next_windows(model, series, lookback=6):
    """
    Predict the next horizon windows based on the latest lookback windows.

    Parameters:
    -----------
    model : nn.Module
        Trained PyTorch model
    series : np.ndarray
        1D array of latest order counts
    lookback : int
        Number of past windows to use

    Returns:
    --------
    prediction : np.ndarray
        Predicted future order counts (horizon)
    """
    latest = series[-lookback:]
    input_tensor = torch.tensor(latest, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)
    with torch.no_grad():
        prediction = model(input_tensor).numpy().flatten()
    return prediction

prediction = predict_next_windows(model, series, lookback)
print("Next window predictions:", prediction)
