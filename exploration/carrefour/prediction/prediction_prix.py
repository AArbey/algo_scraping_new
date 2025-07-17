import pandas as pd
import matplotlib.pyplot as plt
from linearmodels.panel import PanelOLS
import os

# Charger les données
df = pd.read_csv("/home/scraping/algo_scraping/exploration/carrefour/data_cleaned_nprices3.csv", parse_dates=["Timestamp"])
df = df.sort_values(["CodeID", "Timestamp"])

# Lag du prix par produit
df["Price_lag"] = df.groupby("CodeID")["Price"].shift(1)

# Moyenne des prix des algo_suspects pour chaque modèle/jour
algo_avg = (
    df[df["algo_suspect"] == True]
    .groupby(["ModelCode", "Day"])["Price"]
    .mean()
    .reset_index()
    .rename(columns={"Price": "Price_algo_avg_lag"})
)

# Fusion
df = df.merge(algo_avg, on=["ModelCode", "Day"], how="left")

# Nettoyage
df_model = df.dropna(subset=["Price_lag", "Price_algo_avg_lag"]).copy()
df_model = df_model.set_index(["CodeID", "Timestamp"])
df_model["const"] = 1

# Régression
model = PanelOLS(df_model["Price"], df_model[["const", "Price_lag", "Price_algo_avg_lag"]], entity_effects=True)
results = model.fit()
print(results.summary)

# Prédiction
df_model["Price_pred"] = results.fitted_values

# Graphique sur un exemple
#example_id = df_model.index.get_level_values(0).unique()[0]
example_id = 25000
df_ex = df_model.xs(example_id, level="CodeID")

plt.figure(figsize=(10, 5))
plt.plot(df_ex.index, df_ex["Price"], label="Prix réel", marker='o')
plt.plot(df_ex.index, df_ex["Price_pred"], label="Prix prédit", linestyle='--')
plt.title(f"CodeID = {example_id} | Prix réel vs prédit, suspect: 3 chgts, Carrefour")
plt.xlabel("Date")
plt.ylabel("Prix (€)")
plt.legend()
plt.tight_layout()
plt.savefig("/home/scraping/algo_scraping/exploration/carrefour/figures_carrefour/predictions_carrefour/prediction_mod1_CodeID_{example_id}_n3.png")
print("img générée")
plt.close()
