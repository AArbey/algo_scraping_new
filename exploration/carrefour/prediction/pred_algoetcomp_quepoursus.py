import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
from sklearn.metrics import mean_absolute_error
import matplotlib.pyplot as plt

# 1. Charger les données
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned_nprices3.csv", parse_dates=["Timestamp"])

# 2. Trier
df = df.sort_values(["CodeID", "Timestamp"])

# 3. Variables
df["Price_lag1"] = df.groupby("CodeID")["Price"].shift(1)
df["Price_mean_competitor"] = df.groupby(["ModelCode", "Timestamp"])["Price"].transform("mean")
df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]
df["Comp_price_lag1"] = df.groupby("ReadableID")["Price_mean_competitor"].shift(1)

# 4. Ne garder que les vendeurs suspects
df = df[df["algo_suspect"] == True].copy()
df["algo_suspect_int"] = 1  # inutile mais pour rester cohérent
df["Comp_price_lag1_inter"] = df["Comp_price_lag1"]  # = Comp_price_lag1 * 1

# 5. Nettoyage
df_model = df.dropna(subset=["Price_lag1", "Comp_price_lag1"]).copy()
df_model["Date"] = df_model["Timestamp"].dt.date

# 6. Split
train = df_model[df_model["Date"] <= pd.to_datetime("2025-04-29").date()]
test = df_model[df_model["Date"] > pd.to_datetime("2025-04-29").date()]

# 7. PanelOLS setup
train = train.set_index(["CodeID", "Timestamp"])
test = test.set_index(["CodeID", "Timestamp"])
train["const"] = 1
test["const"] = 1

# 8. Régression sur suspects
features = ["const", "Price_lag1", "Comp_price_lag1"]
model = PanelOLS(train["Price"], train[features], entity_effects=True)
results = model.fit()
print(results.summary)

# 9. Prédictions
test["Price_pred"] = results.predict(test[features])
test = test.reset_index()

# 10. Évaluer
mae = mean_absolute_error(test["Price"], test["Price_pred"])
print(f"\n📈 MAE sur vendeurs suspects uniquement : {mae:.2f} €")

# 11. Visualisation (optionnelle)
test["abs_error"] = np.abs(test["Price"] - test["Price_pred"])
daily_error = test.groupby("Timestamp")["abs_error"].mean()

plt.figure(figsize=(10, 4))
daily_error.plot()
plt.title("Erreur absolue moyenne dans le temps (suspects)")
plt.xlabel("Date")
plt.ylabel("Erreur (€)")
plt.tight_layout()
plt.show()
