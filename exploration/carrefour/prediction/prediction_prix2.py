import pandas as pd
import numpy as np
from linearmodels.panel import PanelOLS
from sklearn.metrics import mean_absolute_error
from datetime import timedelta
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Charger les données nettoyées
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned.csv", parse_dates=["Timestamp"])

# 2. Extraire features temporelles
df["hour"] = df["Timestamp"].dt.hour
df["dayofweek"] = df["Timestamp"].dt.dayofweek

# 3. Calcul du prix moyen concurrent par modèle/temps (excluant le vendeur courant)
df["Price_mean_competitor"] = df.groupby(["ModelCode", "Timestamp"])["Price"].transform('mean')
df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]  # diff avec la moyenne

# 4. Tri + création des lags
df = df.sort_values(["CodeID", "Timestamp"])
df["Price_lag1"] = df.groupby("CodeID")["Price"].shift(1)
df["Comp_price_lag1"] = df.groupby("ReadableID")["Price_mean_competitor"].shift(1)

# 5. Si pas algo_suspect → on ignore les concurrents
df["Comp_price_lag1"] = np.where(df["algo_suspect"], df["Comp_price_lag1"], 0.0)

# 6. Supprimer les lignes sans valeurs de lag
df_model = df.dropna(subset=["Price_lag1", "Comp_price_lag1"])

# 7. Séparer train/test (test = après 2025-05-09)
df_model["Date"] = df_model["Timestamp"].dt.date
#train = df_model[df_model["Date"] <= pd.to_datetime("2025-05-09").date()]
#test = df_model[df_model["Date"] > pd.to_datetime("2025-05-09").date()]

train = df_model[df_model["Date"] <= pd.to_datetime("2025-04-29").date()]
test = df_model[df_model["Date"] > pd.to_datetime("2025-04-29").date()]


# 8. Mise en forme pour PanelOLS
train = train.set_index(["CodeID", "Timestamp"])
test = test.set_index(["CodeID", "Timestamp"])

# 9. Ajouter constante
train["const"] = 1
test["const"] = 1

# 10. Sélection des variables
features = ["const", "Price_lag1", "Comp_price_lag1", "hour", "dayofweek"]

# 11. Entraînement du modèle panel avec effets vendeur
model = PanelOLS(train["Price"], train[features], entity_effects=True)
results = model.fit()
print(results.summary)

# 12. Prédiction sur test
test["Price_pred"] = results.predict(test[features])
test = test.reset_index()

# 13. Évaluer la précision
mae = mean_absolute_error(test["Price"], test["Price_pred"])
print(f"\n MAE (Mean Absolute Error) sur données test = {mae:.2f} €")

test["abs_error"] = np.abs(test["Price"] - test["Price_pred"])
daily_error = test.groupby("Timestamp")["abs_error"].mean()

plt.figure(figsize=(10, 4))
daily_error.plot()
plt.title("Erreur absolue moyenne dans le temps")
plt.xlabel("Date")
plt.ylabel("Erreur absolue (MAE)")
plt.tight_layout()
plt.show()

# 14. Afficher quelques exemples de test
print("\n Exemples de prédictions de test :")
print(test[["Timestamp", "ReadableID", "Price", "Price_pred"]].head(10))

# 15. Produits à tracer (les 3 ayant le plus d’observations dans le test)
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
    plt.savefig(f"/home/scraping/algo_scraping/exploration/carrefour/figures_carrefour/predictions_carrefour/prediction_modele2n3_codeid_{code_id}_lgdate.png")
    plt.close()

print("Graphiques enregistrés pour les CodeIDs les plus fréquents dans le test.")"""


### Prédiction selon le code id voulu

# 1. CodeID choisi
target_id = 10008  # à choisir
is_algo = df[df["CodeID"] == target_id]["algo_suspect"].iloc[0]

# 2. Dernière observation
df_id = df[df["CodeID"] == target_id].sort_values("Timestamp")
last_row = df_id.iloc[-1]

# 3. Préparer les features pour demain
tomorrow = last_row["Timestamp"] + timedelta(days=1)
hour = tomorrow.hour
dayofweek = tomorrow.dayofweek

price_lag1 = last_row["Price"]
comp_price_lag1 = df_id["Price_mean_competitor"].iloc[-1] if is_algo else 0.0

# 4. Créer DataFrame au bon format
df_pred = pd.DataFrame([{
    "const": 1,
    "Price_lag1": price_lag1,
    "Comp_price_lag1": comp_price_lag1,
    "hour": hour,
    "dayofweek": dayofweek
}], index=pd.MultiIndex.from_tuples([(target_id, tomorrow)], names=["CodeID", "Timestamp"]))

# 5. Prédiction
predicted_price = results.predict(df_pred).values[0]
print(f"Prix prédit pour le CodeID {target_id} le {tomorrow.strftime('%Y-%m-%d')} : {predicted_price} €")