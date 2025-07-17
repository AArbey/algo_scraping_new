import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
from sklearn.metrics import mean_absolute_error
from datetime import timedelta
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Charger les données nettoyées
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned.csv", parse_dates=["Timestamp"])

# 2. Calcul du prix moyen concurrent par modèle/temps
df["Price_mean_competitor"] = df.groupby(["ModelCode", "Timestamp"])["Price"].transform('mean')
df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]  # différence avec la moyenne

# 3. Tri + création des lags
df = df.sort_values(["CodeID", "Timestamp"])
df["Price_lag1"] = df.groupby("CodeID")["Price"].shift(1)
df["Comp_price_lag1"] = df.groupby("ReadableID")["Price_mean_competitor"].shift(1)

# 4. Créer l’interaction algo_suspect * comp_price
df["algo_suspect_int"] = df["algo_suspect"].astype(int)
df["Comp_price_lag1_inter"] = df["Comp_price_lag1"] * df["algo_suspect_int"]

# 5. Supprimer les lignes sans valeurs de lag
df_model = df.dropna(subset=["Price_lag1", "Comp_price_lag1", "Comp_price_lag1_inter"]).copy()
df_model["Date"] = df_model["Timestamp"].dt.date

# 6. Séparer train/test (train jusqu’au 29/04)
train = df_model[df_model["Date"] <= pd.to_datetime("2025-04-29").date()]
test = df_model[df_model["Date"] > pd.to_datetime("2025-04-29").date()]

# 7. Mise en forme PanelOLS
train = train.set_index(["CodeID", "Timestamp"])
test = test.set_index(["CodeID", "Timestamp"])
train["const"] = 1
test["const"] = 1

# 8. Sélection des variables
features = ["const", "Price_lag1", "Comp_price_lag1", "Comp_price_lag1_inter"]

# 9. Régression panel avec effets vendeur
model = PanelOLS(train["Price"], train[features], entity_effects=True)
results = model.fit()
print(results.summary)

# 10. Prédiction sur test
test["Price_pred"] = results.predict(test[features])
test = test.reset_index()

# 11. Évaluer la précision
mae = mean_absolute_error(test["Price"], test["Price_pred"])
print(f"\n📈 MAE (Mean Absolute Error) sur données test : {mae:.2f} €")

# 12. Graphique des erreurs dans le temps
test["abs_error"] = np.abs(test["Price"] - test["Price_pred"])
daily_error = test.groupby("Timestamp")["abs_error"].mean()

plt.figure(figsize=(10, 4))
daily_error.plot()
plt.title("Erreur absolue moyenne dans le temps")
plt.xlabel("Date")
plt.ylabel("Erreur absolue (MAE)")
plt.tight_layout()
plt.show()

# 13. Exemples
print("\n🔍 Exemples de prédictions de test :")
print(test[["Timestamp", "ReadableID", "Price", "Price_pred"]].head(10))

# 14. Produits à tracer (les 3 plus fréquents dans le test)
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
    plt.savefig(f"/home/scraping/algo_scraping/exploration/carrefour/figures_carrefour/predictions_carrefour/prediction_interaction_codeid_{code_id}.png")
    plt.close()

print("Graphiques enregistrés pour les CodeIDs les plus fréquents dans le test.")
"""

# 15. Prédiction future pour un CodeID cible
target_id = 10008
df_id = df[df["CodeID"] == target_id].sort_values("Timestamp")
is_algo = df_id["algo_suspect"].iloc[-1]
last_row = df_id.iloc[-1]

# Préparer observation future
tomorrow = last_row["Timestamp"] + timedelta(days=1)
price_lag1 = last_row["Price"]
comp_price_lag1 = df_id["Price_mean_competitor"].iloc[-1]
comp_price_lag1_inter = comp_price_lag1 if is_algo else 0.0

df_pred = pd.DataFrame([{
    "const": 1,
    "Price_lag1": price_lag1,
    "Comp_price_lag1": comp_price_lag1,
    "Comp_price_lag1_inter": comp_price_lag1_inter
}], index=pd.MultiIndex.from_tuples([(target_id, tomorrow)], names=["CodeID", "Timestamp"]))

# Prédiction
predicted_price = results.predict(df_pred).values[0]
print(f"\n🔮 Prix prédit pour le CodeID {target_id} le {tomorrow.strftime('%Y-%m-%d')} : {predicted_price} €")
