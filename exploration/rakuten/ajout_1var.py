import pandas as pd
import numpy as np
from collections import defaultdict
from tqdm import tqdm

# Chemins
CSV_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast1.csv"
EXPORT_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_FINAL.csv"

# Chargement
df = pd.read_csv(CSV_PATH, parse_dates=["Timestamp"])
df = df.sort_values("Timestamp").reset_index(drop=True)
df["Model"] = df["ReadableID"].apply(lambda x: x.split("_")[-1])

# Dictionnaire des derniers prix
latest_prices = defaultdict(dict)

# Variables cibles
xs = [1, 2, 4, 5]
results_above = {x: [] for x in xs}
results_below = {x: [] for x in xs}

# Traitement ligne par ligne
for _, row in tqdm(df.iterrows(), total=len(df), desc="Calcul des mean_x_above/below_lag"):
    model = row["Model"]
    curr_id = row["ReadableID"]
    curr_time = row["Timestamp"]
    price_lag = row["Price_lag"]

    snapshot = {
        rid: price
        for rid, (ts, price) in latest_prices[model].items()
        if rid != curr_id and ts < curr_time
    }

    prices = np.array(sorted(snapshot.values()))

    if np.isnan(price_lag) or len(prices) == 0:
        for x in xs:
            results_above[x].append(np.nan)
            results_below[x].append(np.nan)
    else:
        diffs = prices - price_lag
        for x in xs:
            above = prices[diffs > 0]
            below = prices[diffs < 0]
            results_above[x].append(above[:x].mean() if len(above) >= 1 else np.nan)
            results_below[x].append(below[-x:].mean() if len(below) >= 1 else np.nan)

    if not pd.isna(row["Price"]):
        latest_prices[model][curr_id] = (curr_time, row["Price"])

# Ajout au DataFrame
for x in xs:
    df[f"mean_{x}_above_lag"] = results_above[x]
    df[f"mean_{x}_below_lag"] = results_below[x]

# Export
df.to_csv(EXPORT_PATH, index=False)
print(f"✅ Export complété vers : {EXPORT_PATH}")
