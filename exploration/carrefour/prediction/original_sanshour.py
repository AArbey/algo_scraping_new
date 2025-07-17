import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
from sklearn.metrics import mean_absolute_error
from datetime import timedelta
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Charger les données nettoyées
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned_version2.csv", parse_dates=["Timestamp"])

# 2. Calcul du prix moyen concurrent par modèle/temps
df["Price_mean_competitor"] = df.groupby(["ModelCode", "Timestamp"])["Price"].transform('mean')
df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]

# 3. Tri + création des lags
df = df.sort_values(["CodeID", "Timestamp"])
df["Price_lag1"] = df.groupby("CodeID")["Price"].shift(1)
df["Comp_price_lag1"] = df.groupby("ReadableID")["Price_mean_competitor"].shift(1)

# 4. Si pas algo_suspect → ignorer la concurrence
df["Comp_price_lag1"] = np.where(df["algo_suspect"], df["Comp_price_lag1"], 0.0)

# 5. Nettoyage
df_model = df.dropna(subset=["Price_lag1", "Comp_price_lag1"]).copy()
df_model["Date"] = df_model["Timestamp"].dt.date

# 6. Séparer train/test ancienne version
"""
train = df_model[df_model["Date"] <= pd.to_datetime("2025-04-29").date()]
test = df_model[df_model["Date"] > pd.to_datetime("2025-04-29").date()]
"""

# Trier les dates uniques
all_dates = sorted(df_model["Date"].unique())
n_total = len(all_dates)
n_train = int(n_total * 0.48)

# Définir les dates de train/test
train_dates = all_dates[:n_train]
test_dates = all_dates[n_train:]

# Split
train = df_model[df_model["Date"].isin(train_dates)]
test = df_model[df_model["Date"].isin(test_dates)]

# 7. Mise en forme pour PanelOLS
train = train.set_index(["CodeID", "Timestamp"])
test = test.set_index(["CodeID", "Timestamp"])
train["const"] = 1
test["const"] = 1

# 8. Variables retenues
features = ["const", "Price_lag1", "Comp_price_lag1"]

# 9. Régression panel
model = PanelOLS(train["Price"], train[features], entity_effects=True)
results = model.fit()
print(results.summary)

# 10. Prédiction sur test
test["Price_pred"] = results.predict(test[features])
test = test.reset_index()

# 11. Évaluation
mae = mean_absolute_error(test["Price"], test["Price_pred"])
print(f"\n📈 MAE (Mean Absolute Error) sur données test = {mae:.2f} €")

test["abs_error"] = np.abs(test["Price"] - test["Price_pred"])
daily_error = test.groupby("Timestamp")["abs_error"].mean()

plt.figure(figsize=(10, 4))
daily_error.plot()
plt.title("Erreur absolue moyenne dans le temps")
plt.xlabel("Date")
plt.ylabel("Erreur absolue (MAE)")
plt.tight_layout()
plt.show()

# 12. Exemples
print("\n🔍 Exemples de prédictions de test :")
print(test[["Timestamp", "ReadableID", "Price", "Price_pred"]].head(10))

# 13. Tracer quelques produits
top_ids = test["CodeID"].value_counts().nlargest(3).index
top_ids = top_ids.append(pd.Index([25000]))

"""
for code_id in top_ids:
    subset = test[test["CodeID"] == code_id].sort_values("Timestamp")
    
    plt.figure(figsize=(10, 4))
    sns.lineplot(x=subset["Timestamp"], y=subset["Price"], label="Prix réel")
    sns.lineplot(x=subset["Timestamp"], y=subset["Price_pred"], label="Prix prédit", linestyle="--")
    plt.title(f"CodeID {code_id} — Prix réel vs prédit")
    plt.xlabel("Date")
    plt.ylabel("Prix (€)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"/home/scraping/algo_scraping/exploration/carrefour/figures_carrefour/predictions_carrefour/prediction_codeid_{code_id}.png")
    plt.close()

print("Graphiques enregistrés.")
"""

# 14. Prédiction future pour un CodeID
target_id = 10008
df_id = df[df["CodeID"] == target_id].sort_values("Timestamp")
is_algo = df_id["algo_suspect"].iloc[-1]
last_row = df_id.iloc[-1]

tomorrow = last_row["Timestamp"] + timedelta(days=1)
price_lag1 = last_row["Price"]
comp_price_lag1 = df_id["Price_mean_competitor"].iloc[-1] if is_algo else 0.0

df_pred = pd.DataFrame([{
    "const": 1,
    "Price_lag1": price_lag1,
    "Comp_price_lag1": comp_price_lag1
}], index=pd.MultiIndex.from_tuples([(target_id, tomorrow)], names=["CodeID", "Timestamp"]))

predicted_price = results.predict(df_pred).values[0]
print(f"\n🔮 Prix prédit pour le CodeID {target_id} le {tomorrow.strftime('%Y-%m-%d')} : {predicted_price} €")
