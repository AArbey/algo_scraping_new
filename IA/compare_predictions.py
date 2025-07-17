import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from joblib import load
from tensorflow.keras.models import load_model
from sklearn.metrics import mean_absolute_error, mean_squared_error
import seaborn as sns

# Load model, scaler, and test data
model = load_model('phone_price_predictor_model.keras')
scaler = load('scaler.joblib')
X_test = pd.read_csv('X_test.csv')
y_test = pd.read_csv('y_test.csv')

# Prepare categorical and numerical inputs
categorical_cols = ['Platform', 'Product Name', 'Seller', 'Seller Status', 'state']
X_test_cat = [X_test[col].values for col in categorical_cols]
X_test_num = X_test.drop(columns=categorical_cols).values

# Predict
predictions = model.predict(X_test_cat + [X_test_num])

# Inverse scaling for predictions and actual prices
def inverse_scale_prices(scaler, values):
    dummy = np.zeros((len(values), scaler.n_features_in_))
    dummy[:, 0] = values.flatten()
    return scaler.inverse_transform(dummy)[:, 0]

predictions_real = inverse_scale_prices(scaler, predictions)
y_test_real = inverse_scale_prices(scaler, y_test.values)

# Remove prices above €3000 (actual AND predicted)
mask = (y_test_real <= 3000) & (predictions_real <= 3000)
predictions_real = predictions_real[mask]
y_test_real     = y_test_real[mask]

# Create comparison DataFrame
comparison_df = pd.DataFrame({
    'Actual Price (€)': y_test_real,
    'Predicted Price (€)': predictions_real,
    'Absolute Error (€)': np.abs(y_test_real - predictions_real)
})

# Print metrics and samples
print("First 10 Predictions vs Actual Prices:")
print(comparison_df.head(10))

mae = mean_absolute_error(y_test_real, predictions_real)
rmse = np.sqrt(mean_squared_error(y_test_real, predictions_real))
print(f"\nMAE: €{mae:.2f}")
print(f"RMSE: €{rmse:.2f}")

# Ajout de la classification « bonne » prédiction
threshold = 50  # euros
comparison_df['Good Prediction'] = comparison_df['Absolute Error (€)'] <= threshold
good_count = comparison_df['Good Prediction'].sum()
total_count = len(comparison_df)
bad_count = total_count - good_count
print(f"\nGood predictions (error ≤ {threshold}€): {good_count}/{total_count} "
      f"({good_count/total_count:.1%})")

# Plotting with Seaborn for cleaner visuals
sns.set_theme(style='whitegrid')

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

# Scatter plot: Actual vs Predicted
sns.scatterplot(
    data=comparison_df,
    x='Actual Price (€)',
    y='Predicted Price (€)',
    hue='Good Prediction',
    palette={True: 'green', False: 'red'},
    s=30,
    alpha=0.6,
    ax=ax1
)
ax1.plot(
    [comparison_df['Actual Price (€)'].min(), comparison_df['Actual Price (€)'].max()],
    [comparison_df['Actual Price (€)'].min(), comparison_df['Actual Price (€)'].max()],
    '--k',
    lw=2,
)
ax1.text(
    0.95, 0.05,
    f"Good: {good_count/total_count:.1%}",
    transform=ax1.transAxes,
    ha='right',
    va='bottom',
    color='green',
    fontsize=12,
)
ax1.set_title('Actual vs Predicted Prices')
ax1.set_xlabel('Actual Price (€)')
ax1.set_ylabel('Predicted Price (€)')
ax1.legend(title=f'Bonne prédiction (≤ {threshold}€)', loc='upper left')

# Histogram: Distribution of Absolute Errors
sns.histplot(
    data=comparison_df,
    x='Absolute Error (€)',
    bins=60,
    color='blue',
    edgecolor='black',
    alpha=0.7,
    ax=ax2
)
ax2.axvline(threshold, color='green', linestyle='--', label=f'Threshold {threshold}€')
ax2.set_title('Distribution des erreurs absolues')
ax2.set_xlim(0, 300)
ax2.set_xticks(np.arange(0, 301, 20))
plt.setp(ax2.get_xticklabels(), rotation=45)
ax2.set_xlabel('Absolute Error (€)')
ax2.set_ylabel('Count')
ax2.legend()

fig.tight_layout()
plt.show()