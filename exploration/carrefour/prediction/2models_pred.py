import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
from sklearn.metrics import mean_absolute_error

# Chargement des données
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned.csv", parse_dates=["Timestamp"])

# Construction des variables
df = df.sort_values(["CodeID", "Timestamp"])
df["Price_lag1"] = df.groupby("CodeID")["Price"].shift(1)
df["Price_mean_competitor"] = df.groupby(["ModelCode", "Timestamp"])["Price"].transform('mean')
df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]
df["Comp_price_lag1"] = df.groupby("ReadableID")["Price_mean_competitor"].shift(1)
df["Comp_price_lag1"] = np.where(df["algo_suspect"], df["Comp_price_lag1"], 0.0)

# Nettoyage
df_model = df.dropna(subset=["Price_lag1", "Comp_price_lag1"]).copy()
df_model.loc[:, "Date"] = df_model["Timestamp"].dt.date

# Split train/test (train jusqu'au 29/04)
train = df_model[df_model["Date"] <= pd.to_datetime("2025-04-29").date()]
test = df_model[df_model["Date"] > pd.to_datetime("2025-04-29").date()]

# Mise en forme PanelOLS
train = train.set_index(["CodeID", "Timestamp"])
test = test.set_index(["CodeID", "Timestamp"])
train["const"] = 1
test["const"] = 1

### 🔹 Modèle 1 : sans Comp_price_lag1
features_1 = ["const", "Price_lag1"]
model_1 = PanelOLS(train["Price"], train[features_1], entity_effects=True).fit()
test["Price_pred_1"] = model_1.predict(test[features_1])
mae_1 = mean_absolute_error(test["Price"], test["Price_pred_1"])
print(f"📊 MAE modèle sans Comp_price_lag1 : {mae_1:.2f} €")

### 🔹 Modèle 2 : avec Comp_price_lag1
features_2 = ["const", "Price_lag1", "Comp_price_lag1"]
model_2 = PanelOLS(train["Price"], train[features_2], entity_effects=True).fit()
test["Price_pred_2"] = model_2.predict(test[features_2])
mae_2 = mean_absolute_error(test["Price"], test["Price_pred_2"])
print(f"📈 MAE modèle avec Comp_price_lag1 : {mae_2:.2f} €")

### Résumé comparatif
gain = mae_1 - mae_2
print("\n🔍 Comparaison des modèles :")
print(f"→ Amélioration MAE : {gain:.2f} € ({(gain/mae_1*100):.2f}%)")
