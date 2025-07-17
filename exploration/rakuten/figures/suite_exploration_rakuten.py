import pandas as pd
import numpy as np

# Charger le CSV
df = pd.read_csv("/home/scraping/algo_scraping/exploration/rakuten/NEWdata_cleaned_rakuten.csv", parse_dates=["Timestamp"])
df["Day"] = df["Timestamp"].dt.date

def enrich_pricing_strategies(df):
    result = []
    for day, group in df.groupby("Day"):
        group = group.copy()
        group_sorted = group.sort_values("Price").reset_index(drop=True)

        # Moyenne des 3 et 5 prix les plus bas
        group["mean_3_min"] = group_sorted["Price"].nsmallest(3).mean()
        group["mean_5_min"] = group_sorted["Price"].nsmallest(5).mean()

        # Moyenne des 2 prix les plus élevés
        group["mean_2_max"] = group_sorted["Price"].nlargest(2).mean()

        # Médiane des prix
        group["median_price"] = group["Price"].median()

        # Moyenne des 3 prix les plus proches de chaque observation
        group["mean_3_closest"] = group["Price"].apply(
            lambda p: group.loc[np.abs(group["Price"] - p).nsmallest(4).index]
                         .drop(group[group["Price"] == p].index[0])["Price"].mean()
            if len(group) > 3 else np.nan
        )

        result.append(group)
    return pd.concat(result, ignore_index=True)

# Appliquer
df_enriched = enrich_pricing_strategies(df)

# Sauvegarde si besoin
df_enriched.to_csv("/home/scraping/algo_scraping/exploration/rakuten/NEWdata_enriched_rakuten.csv", index=False)
