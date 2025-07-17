import pandas as pd
import numpy as np
import statsmodels.api as sm

# Charger les données nettoyées
df = pd.read_csv("/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_plus5.csv", parse_dates=["Timestamp"])
df["Day"] = df["Timestamp"].dt.date

# Trier pour avoir un ordre chronologique
df = df.sort_values(["ReadableID", "Timestamp"])
variable = "mean_5_above_lag"
# Régression uniquement sur les vendeurs suspects
flag = True
df_model = df[(df["algo_suspect"] == flag) & (df[variable].notna())][["Price", variable]].copy()

# Régression linéaire
X = sm.add_constant(df_model[[variable]])
y = df_model["Price"]
model = sm.OLS(y, X).fit()

# Résultats
print(model.summary())
print(f"\n✅ Résumé pour stratégie corrigée : {variable} (seulement pour prix du même modèle), pour algo_suspect : {flag}")
print(f"R² : {model.rsquared:.4f}")
print(f"MAE : {np.mean(np.abs(model.predict(X) - y)):.2f} €")
