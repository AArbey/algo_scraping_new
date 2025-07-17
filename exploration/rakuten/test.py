from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import pandas as pd
import numpy as np
import statsmodels.api as sm


df = pd.read_csv("/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_plus5.csv", parse_dates=["Timestamp"])
variable = "mean_5_above_lag"

# Supprimer les lignes avec des NaN dans les colonnes de régression
df_model = df.dropna(subset=[variable, 'Price'])

# Régression uniquement sur les vendeurs suspects
flag = True
df_model = df_model[(df_model["algo_suspect"] == flag)]

# Séparation des variables
X = df_model[[variable]]
y = df_model["Price"]

# Séparation en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Modèle Ridge avec régularisation L2
ridge_model = Ridge(alpha=1.0)  # L'alpha est un hyperparamètre à ajuster
ridge_model.fit(X_train, y_train)

# Prédictions et évaluation
y_pred = ridge_model.predict(X_test)
mae_ridge = mean_absolute_error(y_test, y_pred)

print(f"MAE pour Ridge : {mae_ridge:.2f} €")
