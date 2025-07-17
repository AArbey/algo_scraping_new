import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Model, load_model
from tensorflow.keras.layers import Input, LSTM, Dense, Embedding, Concatenate, Flatten
from tensorflow.keras.optimizers import Adam
from sklearn.metrics import mean_absolute_error, mean_squared_error
from joblib import dump

import matplotlib.pyplot as plt

# Load combined data
df = pd.read_csv('data/parquet_data_cleaned.csv', sep='|', low_memory=False)

df['Timestamp'] = pd.to_datetime({
    'year': df['Year'],
    'month': df['Month'],
    'day': df['Day'],
    'hour': df['Hour'],
    'minute': df['Minute'],
    'second': df['Second']
})

df.dropna(subset=['Timestamp'], inplace=True)
df['Seller Status'] = df['Seller Status'].fillna('Unknown')

# Drop rows missing critical values
# e.g., drop if Price or TF_ENABLE_ONEDNN_OPTS=0 missing
df = df.dropna(subset=['Price', 'Product Name'])
# Optionally fill missing delivery fee with 0
df['Delivery Fee'] = df['Delivery Fee'].fillna(0)
df['Seller Rating'] = df['Seller Rating'].fillna(5)
# add special_period (0/1) and fill missing
df['special_period'] = df['special_period'].fillna(0)

# Remove outliers: drop prices over 3000
df = df[df['Price'] <= 3000]


# Sort by time
df.sort_values('Timestamp', inplace=True)

# Encode categorical features
label_encoders = {}
categorical_cols = ['Platform', 'Product Name', 'Seller', 'Seller Status', 'state']
col_layer_names = {col: col.replace(' ', '_') for col in categorical_cols}
for col in categorical_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    label_encoders[col] = le

# Create lag features
look_back = 7
# Shift price by seller+product
df['Lag_Price_1'] = df.groupby(['Product Name', 'Seller'])['Price'].shift(1)
for i in range(2, look_back+1):
    df[f'Lag_Price_{i}'] = df.groupby(['Product Name', 'Seller'])['Price'].shift(i)
# Drop rows with missing lag values
df.dropna(inplace=True)

# Extract temporal feature
df['DayOfWeek'] = df['Timestamp'].dt.dayofweek

# Normalize numerical features (include Price here so scaler can invert it later)
scaler = MinMaxScaler()
numerical_cols = [
    'Price', 'Seller Rating', 'Delivery Fee',
    'Year', 'Month', 'Day', 'Hour', 'Minute', 'Second', 'DayOfWeek',
    'special_period'
]
df[numerical_cols] = scaler.fit_transform(df[numerical_cols])
dump(scaler, 'scaler.joblib')

# Prepare features and target
feature_cols = (
    categorical_cols
    + ['Seller Rating', 'Delivery Fee', 'Year', 'Month', 'Day', 'Hour', 'Minute', 'Second', 'DayOfWeek', 'special_period']
    + [f'Lag_Price_{i}' for i in range(1, look_back+1)]
)
X = df[feature_cols]
y = df['Price']

# Train/test split
split_idx = int(len(df) * 0.75)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

# Save test data
X_test.to_csv('X_test.csv', index=False)
y_test.to_csv('y_test.csv', index=False)

# Embedding dimensions
embedding_dims = {
    'Platform': 3,
    'Product Name': 10,
    'Seller': 8,
    'Seller Status': 3,
    'state': 3
}

# Build model inputs
inputs = []
embeddings = []
for col in categorical_cols:
    safe = col_layer_names[col]
    inp = Input(shape=(1,), name=f'input_{safe}')
    embed = Embedding(
        input_dim=len(label_encoders[col].classes_),
        output_dim=embedding_dims[col],
        name=f'embedding_{safe}'
    )(inp)
    embeddings.append(Flatten()(embed))
    inputs.append(inp)
# Numerical input
dum = len(feature_cols) - len(categorical_cols)
num_inp = Input(shape=(dum,), name='numerical_input')
inputs.append(num_inp)

# Merge
total = Concatenate()(embeddings + [num_inp])
d1 = Dense(64, activation='relu')(total)
out = Dense(1, activation='linear')(d1)

model_path = 'phone_price_predictor_model.keras'
if os.path.exists(model_path):
    print('Loading existing model...')
    model = load_model(model_path)
else:
    model = Model(inputs=inputs, outputs=out)
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse')

# Prepare inputs
X_train_cat = [X_train[col].values for col in categorical_cols]
X_train_num = X_train.drop(columns=categorical_cols).values
X_test_cat = [X_test[col].values for col in categorical_cols]
X_test_num = X_test.drop(columns=categorical_cols).values

# Train
model.fit(
    X_train_cat + [X_train_num], y_train.values,
    epochs=40, batch_size=128, validation_split=0.2, verbose=1
)
# Save
model.save(model_path)
print(f'Model saved to {model_path}')
# Evaluate
test_loss = model.evaluate(X_test_cat + [X_test_num], y_test.values)
print(f'Test Loss: {test_loss}')
# Predict
predictions = model.predict(X_test_cat + [X_test_num])
