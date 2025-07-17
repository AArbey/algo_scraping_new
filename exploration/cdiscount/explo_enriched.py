import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist


# Charger le CSV
df = pd.read_csv("/home/scraping/algo_scraping/exploration/cdiscount/data_cleaned_cdiscount.csv", parse_dates=["Timestamp"])
df["Day"] = df["Timestamp"].dt.date

def enrich_pricing_strategies(df):
    result = []
    for day, group in df.groupby("Day"):
        group = group.copy()
        group_sorted = group.sort_values("Price").reset_index(drop=True)

        group["mean_3_min"] = group_sorted["Price"].nsmallest(3).mean()
        group["mean_5_min"] = group_sorted["Price"].nsmallest(5).mean()
        group["mean_2_max"] = group_sorted["Price"].nlargest(2).mean()
        group["median_price"] = group["Price"].median()

        # Optimisation du calcul des 3 plus proches voisins
        prices = group["Price"].values.reshape(-1, 1)
        dists = cdist(prices, prices)
        np.fill_diagonal(dists, np.inf)  # On ne veut pas se comparer à soi-même
        idx = np.argsort(dists, axis=1)[:, :3]
        mean_3_closest = np.take_along_axis(prices.flatten()[None, :], idx, axis=1).mean(axis=1)
        group["mean_3_closest"] = mean_3_closest

        result.append(group)
    return pd.concat(result, ignore_index=True)

# Appliquer
df_enriched = enrich_pricing_strategies(df)

# Sauvegarde si besoin
df_enriched.to_csv("/home/scraping/algo_scraping/exploration/cdiscount/NEWdata_enriched_cdiscount.csv", index=False)
