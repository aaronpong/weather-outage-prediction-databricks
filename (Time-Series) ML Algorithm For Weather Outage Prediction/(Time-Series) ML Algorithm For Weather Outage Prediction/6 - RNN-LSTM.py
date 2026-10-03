# Databricks notebook source
outage_data = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://databricks-bucket-ece606/datasets/processed/Energy-OE/Combined_Summary.csv")

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

outage_data_standardized = outage_data.withColumn(
    "wind_speed", 
    coalesce(outage_data["wind_speed"], lit(0.0))
).dropna(subset=['temp_min', 'temp_max', 'weather_outage'])

# COMMAND ----------

!pip install torch

# COMMAND ----------

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
import numpy as np
from datetime import datetime
from pyspark.sql.functions import month, dayofweek, to_date
from sklearn.preprocessing import MinMaxScaler

df_with_time = outage_data_standardized.withColumn(
    "month", month(to_date("date_event_began"))
).withColumn(
    "day_of_week", dayofweek(to_date("date_event_began"))
)

# Convert to pandas for easier preprocessing with PyTorch
pandas_df = df_with_time.toPandas()

# Normalize numerical features
scaler = MinMaxScaler()
features = ['temp_min', 'temp_max', 'wind_speed', 'month', 'day_of_week']
pandas_df[features] = scaler.fit_transform(pandas_df[features])

def create_sequences(data, seq_length=7):
    xs = []
    ys = []
    for i in range(len(data) - seq_length):
        x = data[i:(i + seq_length), :]
        y = data[i + seq_length, -1]  # target is weather_outage
        xs.append(x)
        ys.append(y)
    return np.array(xs), np.array(ys)

# Create PyTorch dataset
class WeatherDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.FloatTensor(X)
        self.y = torch.FloatTensor(y)
    
    def __len__(self):
        return len(self.X)
    
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

# Define RNN model
class WeatherRNN(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers):
        super(WeatherRNN, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)
        self.sigmoid = nn.Sigmoid()
        
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        out = self.sigmoid(out)
        return out

# Prepare data
feature_data = pandas_df[features].values
target_data = pandas_df['weather_outage'].values
X_seq, y_seq = create_sequences(feature_data)

# Split data
train_size = int(0.8 * len(X_seq))
X_train, X_test = X_seq[:train_size], X_seq[train_size:]
y_train, y_test = y_seq[:train_size], y_seq[train_size:]

# Create data loaders
train_dataset = WeatherDataset(X_train, y_train)
test_dataset = WeatherDataset(X_test, y_test)
train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False)

# Initialize model
input_size = len(features)
hidden_size = 32
num_layers = 1
model = WeatherRNN(input_size, hidden_size, num_layers)

# Training parameters
criterion = nn.BCELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
num_epochs = 50

# Training loop
for epoch in range(num_epochs):
    model.train()
    total_loss = 0
    for batch_X, batch_y in train_loader:
        optimizer.zero_grad()
        outputs = model(batch_X).squeeze()
        loss = criterion(outputs, batch_y)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    
    if (epoch + 1) % 10 == 0:
        print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {total_loss/len(train_loader):.4f}')


model.eval()
y_pred = []
y_true = []
with torch.no_grad():
    for batch_X, batch_y in test_loader:
        outputs = model(batch_X).squeeze()
        # Convert to binary (0 or 1)
        predictions = (outputs >= 0.5).int()
        y_pred.extend(predictions.numpy().astype(int))
        y_true.extend(batch_y.numpy().astype(int))

# Convert to numpy arrays of integers
y_pred = np.array(y_pred, dtype=int)
y_true = np.array(y_true, dtype=int)

# Calculate metrics
from sklearn.metrics import accuracy_score, precision_score, recall_score
accuracy = accuracy_score(y_true, y_pred)
precision = precision_score(y_true, y_pred, zero_division=0)
recall = recall_score(y_true, y_pred, zero_division=0)

print("\nRNN Results:")
print(f"Accuracy: {accuracy * 100:.2f}%")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")

from sklearn.metrics import confusion_matrix
conf_matrix = confusion_matrix(y_true, y_pred)
print("\nConfusion Matrix:")
print(conf_matrix)

# COMMAND ----------

