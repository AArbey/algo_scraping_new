import pandas as pd
import numpy as np
import statsmodels.api as sm

# Charger les données nettoyées
df = pd.read_csv("/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_FINAL.csv", parse_dates=["Timestamp"])
df["Day"] = df["Timestamp"].dt.date

# Trier pour avoir un ordre chronologique
df = df.sort_values(["ReadableID", "Timestamp"])
df["offertype"] = (df["offertype"] == "NewCondition").astype(int)
df["algo_suspect"] = df["algo_suspect"].astype(int)

# Vérification des types de colonnes
#print("Types de colonnes avant conversion:")
#print(df.dtypes)

variable = "algo_suspect"
variable2 = "offertype"
variable3 ="shipcost"
variableprix="mean_3_above_lag"
df["interaction_3above"] = df[variableprix] * df["algo_suspect"]
variable4="interaction_3above"
vars_model=[variable, variable2, variable3, variableprix] 

# Convertir les colonnes en numériques si nécessaire (en particulier celles utilisées dans la régression)
for var in vars_model:
    df[var] = pd.to_numeric(df[var], errors='coerce')

df['Price'] = pd.to_numeric(df['Price'], errors='coerce')  # Conversion en numérique

# Supprimer les lignes avec des NaN dans les colonnes de régression
df_model = df.dropna(subset=vars_model + ['Price'])

df_model = df_model[["Price"] + vars_model].copy()

# Vérification après conversion des colonnes
#print("\nTypes de colonnes après conversion:")
#print(df_model.dtypes)

# Régression linéaire
X = sm.add_constant(df_model[vars_model])  # Ajouter les variables ici
y = df_model["Price"]
model = sm.OLS(y, X).fit()

# Résultats
print(model.summary())
print(f"\n✅ Résumé pour stratégie corrigée avec ces variables : {', '.join(vars_model)}")
print(f"R² : {model.rsquared:.4f}")
print(f"MAE : {np.mean(np.abs(model.predict(X) - y)):.2f} €")

# Statistique demandée : % de prédictions à moins de 50 € de l’observé
y_pred = model.predict(X)
errors = np.abs(y_pred - y)
within_50 = (errors <= 50).sum()
total = len(errors)
pourcentage = 100 * within_50 / total

print(f"\n🔍 Prédictions à moins de 50 € du prix réel : {within_50} / {total} ({pourcentage:.2f}%)")


# Créer un tableau avec les valeurs réelles, prédites et l’erreur absolue
df_results = df_model.copy()
df_results["prediction"] = y_pred
df_results["absolute_error"] = np.abs(y_pred - y)

# Afficher les premières lignes
print(df_results[["Price", "prediction", "absolute_error"]].head())

# Sauvegarder le tableau si souhaité
df_results.to_csv("/home/scraping/algo_scraping/exploration/rakuten/prediction_errors.csv", index=False)
print("✅ Tableau des erreurs exporté vers prediction_errors.csv")