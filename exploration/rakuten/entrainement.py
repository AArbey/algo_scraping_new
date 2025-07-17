import pandas as pd
import numpy as np
import statsmodels.api as sm

# Charger les données enrichies
df = pd.read_csv("/home/scraping/algo_scraping/exploration/rakuten/NEWdata_enriched_rakuten.csv")

# Variables explicatives (stratégies)
features = ["mean_3_min", "mean_5_min", "mean_3_closest", "median_price", "mean_2_max"]

# Préparer la sortie
results = []

for var in features:
    df_model = df[["Price", var]].dropna()
    
    X = sm.add_constant(df_model[[var]])  # ajoute l'intercept
    y = df_model["Price"]
    
    model = sm.OLS(y, X).fit()
    y_pred = model.predict(X)
    
    mae = np.mean(np.abs(y - y_pred))
    r2 = model.rsquared
    coef = model.params[var]
    
    results.append({
        "Variable": var,
        "Coef": round(coef, 4),
        "R²": round(r2, 4),
        "MAE (€)": round(mae, 2)
    })

# Affichage propre sous forme de DataFrame
results_df = pd.DataFrame(results).sort_values("MAE (€)")
print(results_df.to_string(index=False))
