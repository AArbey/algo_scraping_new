import pandas as pd
import numpy as np
from collections import defaultdict
from tqdm import tqdm
import time


# Fichiers
CSV_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_cleaned_rakuten_lag3.csv"
EXPORT_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast1.csv"

# Chargement
df = pd.read_csv(CSV_PATH, parse_dates=["Timestamp"])
df = df.sort_values("Timestamp").reset_index(drop=True)

# Extraction du modèle depuis ReadableID
df["Model"] = df["ReadableID"].apply(lambda x: x.split("_")[-1])

# Dictionnaire pour stocker les derniers prix avant chaque ligne : {model: {readable_id: (timestamp, price)}}
latest_prices = defaultdict(dict)

# Listes de résultats
results = {
    "mean_min_3_lag": [],
    "mean_min_5_lag": [],
    "mean_max_3_lag": [],
    "mean_max_5_lag": [],
    "median_lag": [],
    "mean_nearest_lag": [],
    "mean_3_above_lag": [],
    "mean_3_below_lag": [],
}

start_time = time.time()

for _, row in tqdm(df.iterrows(), total=len(df), desc="Calcul des variables"):
    model = row["Model"]
    curr_id = row["ReadableID"]
    curr_time = row["Timestamp"]
    price_lag = row["Price_lag"]

    # Récupérer les derniers prix AVANT t de tous les autres vendeurs sur le même modèle
    snapshot = {
        rid: price
        for rid, (ts, price) in latest_prices[model].items()
        if rid != curr_id and ts < curr_time
    }

    prices = list(snapshot.values())
    prices_sorted = sorted(prices)
    prices_array = np.array(prices_sorted)

    def mean_nearest(array, target, above=False, below=False):
        if target is np.nan or not array.size:
            return np.nan
        diffs = array - target
        if above:
            candidates = array[diffs > 0]
            return candidates[:3].mean() if len(candidates) >= 1 else np.nan
        elif below:
            candidates = array[diffs < 0][::-1]
            return candidates[:3].mean() if len(candidates) >= 1 else np.nan
        else:
            idx_sorted = np.argsort(np.abs(array - target))
            return array[idx_sorted[:3]].mean()

    results["mean_min_3_lag"].append(np.mean(prices_sorted[:3]) if len(prices_sorted) >= 1 else np.nan)
    results["mean_min_5_lag"].append(np.mean(prices_sorted[:5]) if len(prices_sorted) >= 1 else np.nan)
    results["mean_max_3_lag"].append(np.mean(prices_sorted[-3:]) if len(prices_sorted) >= 1 else np.nan)
    results["mean_max_5_lag"].append(np.mean(prices_sorted[-5:]) if len(prices_sorted) >= 1 else np.nan)
    results["median_lag"].append(np.median(prices_sorted) if len(prices_sorted) >= 1 else np.nan)

    if np.isnan(price_lag):
        results["mean_nearest_lag"].append(np.nan)
        results["mean_3_above_lag"].append(np.nan)
        results["mean_3_below_lag"].append(np.nan)
    else:
        results["mean_nearest_lag"].append(mean_nearest(prices_array, price_lag))
        results["mean_3_above_lag"].append(mean_nearest(prices_array, price_lag, above=True))
        results["mean_3_below_lag"].append(mean_nearest(prices_array, price_lag, below=True))

    if not pd.isna(row["Price"]):
        latest_prices[model][curr_id] = (curr_time, row["Price"])



# Ajout au DataFrame
for key, values in results.items():
    df[key] = values

# Export
df.to_csv(EXPORT_PATH, index=False)
end_time = time.time()
print(f"\n⏱️ Temps total : {round((end_time - start_time)/60, 2)} minutes")
print(f"✅ Export terminé : {EXPORT_PATH}")
