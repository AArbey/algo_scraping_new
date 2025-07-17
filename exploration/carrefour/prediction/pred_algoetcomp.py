import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
from sklearn.metrics import mean_absolute_error
import matplotlib.pyplot as plt

# 1. Charger les données
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned_nprices3.csv", parse_dates=["Timestamp"])

# 2. Trier les données par produit et temps
df = df.sort_values(["CodeID", "Timestamp"])

# 3. Créer les variables de lag
df["Price_lag1"] = df.groupby("CodeID")["Price"].shift(1)

# 4. Calcul du prix moyen concurrent pour chaque modèle/timestamp
df["Price_mean_competitor"] = df.groupby(["ModelCode", "Timestamp"])["Price"].transform("mean")
df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]  # écart avec son propre prix

# 5. Lag du prix concurrent
df["Comp_price_lag1"] = df.groupby("ReadableID")["Price_mean_competitor"].shift(1)

# 6. Ajouter variable booléenne algo_suspect (int)
df["algo_suspect_int"] = df["algo_suspect"].astype(int)

# 7. Interaction explicite
df["Comp_price_lag1_inter"] = df["Comp_price_lag1"] * df["algo_suspect_int"]

# 8. Nettoyage : supprimer les lignes incomplètes
df_model = df.dropna(subset=["Price_lag1", "Comp_price_lag1", "Comp_price_lag1_inter"]).copy()
df_model["Date"] = df_model["Timestamp"].dt.date

# 9. Split train/test (train jusqu'au 29 avril)
train = df_model[df_model["Date"] <= pd.to_datetime("2025-04-29").date()]
test = df_model[df_model["Date"] > pd.to_datetime("2025-04-29").date()]

# 10. Mise en forme PanelOLS
train = train.set_index(["CodeID", "Timestamp"])
test = test.set_index(["CodeID", "Timestamp"])
train["const"] = 1
test["const"] = 1

# 11. Régression avec interaction
features = ["const", "Price_lag1", "Comp_price_lag1", "Comp_price_lag1_inter"]
model = PanelOLS(train["Price"], train[features], entity_effects=True)
results = model.fit()
print(results.summary)

# 12. Prédiction sur test
test["Price_pred"] = results.predict(test[features])
test = test.reset_index()

# 13. Évaluation de la précision
mae = mean_absolute_error(test["Price"], test["Price_pred"])
print(f"\n📈 MAE sur données test : {mae:.2f} €")

# 14. Afficher quelques prédictions
print(test[["Timestamp", "ReadableID", "Price", "Price_pred"]].head())

# 15. Visualisation de l'erreur dans le temps
test["abs_error"] = np.abs(test["Price"] - test["Price_pred"])
daily_error = test.groupby("Timestamp")["abs_error"].mean()

plt.figure(figsize=(10, 4))
daily_error.plot()
plt.title("Erreur absolue moyenne par jour (MAE)")
plt.xlabel("Date")
plt.ylabel("Erreur (€)")
plt.tight_layout()
plt.show()
