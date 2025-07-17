import pandas as pd
import numpy as np
import os

# Charger les données
CSV_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_cleaned_rakuten_lag2.csv"
EXPORT_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables.csv"

# Charger le CSV nettoyé
df = pd.read_csv(CSV_PATH, parse_dates=["Timestamp"])
df["Day"] = df["Timestamp"].dt.date


def compute_lagged_features(row, df_day, n=3, feature="min"):
    """
    Calcul des variables de prix pour la période t-1 :
    - feature : "min" pour les prix les plus bas, "max" pour les prix les plus hauts, "median" pour la médiane, etc.
    """
    others = df_day[(df_day["ReadableID"] != row["ReadableID"]) & (df_day["Timestamp"] < row["Timestamp"])]

    # Filtrage pour garder le dernier prix avant t pour chaque vendeur-modèle
    others_last_price = others.sort_values("Timestamp", ascending=False).drop_duplicates(subset="ReadableID", keep="first")

    if feature == "min":
        # Calcul des n prix les plus bas
        lag_prices = others_last_price.nsmallest(n, 'Price', keep="first")
    elif feature == "max":
        # Calcul des n prix les plus hauts
        lag_prices = others_last_price.nlargest(n, 'Price', keep="first")
    elif feature == "median":
        # Calcul de la médiane
        return others_last_price['Price'].median() if not others_last_price.empty else np.nan
    elif feature == "nearest":
        # Calcul des 3 prix les plus proches de Price_lag
        others_last_price["distance"] = np.abs(others_last_price["Price"] - row["Price_lag"])
        lag_prices = others_last_price.nsmallest(n, "distance", keep="first")
    elif feature == "above":
        # Calcul des 3 prix juste au-dessus de Price_lag
        higher_prices = others_last_price[others_last_price["Price"] > row["Price_lag"]]
        lag_prices = higher_prices.nsmallest(n, "Price", keep="first")
    elif feature == "below":
        # Calcul des 3 prix juste en-dessous de Price_lag
        lower_prices = others_last_price[others_last_price["Price"] < row["Price_lag"]]
        lag_prices = lower_prices.nlargest(n, "Price", keep="first")

    return lag_prices["Price"].mean() if not lag_prices.empty else np.nan


def add_lagged_features(df):
    """
    Fonction pour ajouter les nouvelles variables basées sur les prix à t-1 pour chaque modèle-vendeur.
    """
    df["mean_min_3_lag"] = df.apply(lambda row: compute_lagged_features(row, df, n=3, feature="min"), axis=1)
    df["mean_min_5_lag"] = df.apply(lambda row: compute_lagged_features(row, df, n=5, feature="min"), axis=1)
    df["mean_max_3_lag"] = df.apply(lambda row: compute_lagged_features(row, df, n=3, feature="max"), axis=1)
    df["mean_max_5_lag"] = df.apply(lambda row: compute_lagged_features(row, df, n=5, feature="max"), axis=1)
    df["median_lag"] = df.apply(lambda row: compute_lagged_features(row, df, feature="median"), axis=1)
    df["mean_nearest_lag"] = df.apply(lambda row: compute_lagged_features(row, df, feature="nearest"), axis=1)
    df["mean_3_above_lag"] = df.apply(lambda row: compute_lagged_features(row, df, feature="above"), axis=1)
    df["mean_3_below_lag"] = df.apply(lambda row: compute_lagged_features(row, df, feature="below"), axis=1)

    return df


# Ajouter les variables au DataFrame
df = add_lagged_features(df)

# Exporter le fichier avec les nouvelles variables
df.to_csv(EXPORT_PATH, index=False)

print(f"Fichier exporté avec les nouvelles variables dans {EXPORT_PATH}")
