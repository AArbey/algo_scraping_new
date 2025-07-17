import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import hashlib
import re
from datetime import datetime
import os

CSV_PATH = "/home/scraping/algo_scraping/RAKUTEN/Rakuten_data.csv"
EXPORT_PATH = "/home/scraping/algo_scraping/exploration/rakuten/data_cleaned_rakuten.csv"
FIG_DIR = "/home/scraping/algo_scraping/exploration/rakuten/figures"
os.makedirs(FIG_DIR, exist_ok=True)
os.makedirs(os.path.dirname(EXPORT_PATH), exist_ok=True)

def load_and_clean_data(path):
    # Chargement des données avec les colonnes spécifiques à Rakuten
    df = pd.read_csv(path, 
                names=["Platform","Idsmart","Product","Timestamp","Price","Delivery","Rating","Ratingnb","Status","Details","Shipcountry","Sellercountry","Seller"],
                skiprows=1, 
                on_bad_lines='skip',
                sep=',')

    df = df[df["Price"].notna()]
    df["Price"] = df["Price"].astype(float)

    df["Rating"] = df["Rating"].astype(float)


    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", dayfirst=True)
    df = df[df["Timestamp"].notnull()]
    df["Day"] = df["Timestamp"].dt.floor("D")
    

    def extract_state(name):
        name = str(name).lower()
        res = ""
        if "excellent" in name:
            res+="e"
        if "bon état" in name:
            res+="b"
        if "neuf" in name:
            res+="n"
        if "reconditionné" in name:
            res+="r"
        return res
    
    df["State"] = df["Product"].apply(extract_state)
    
    return df

def generate_model_code(product_name):
    product_name = str(product_name).lower().replace("apple", "").replace("iphone", "ip").replace(" ", "")
    
    gen_match = re.search(r'ip(\d{2})', product_name)
    gen = gen_match.group(1) if gen_match else "xx"
    
    version = ""
    if "promax" in product_name:
        version = "promax"
    elif "pro" in product_name:
        version = "pro"
    elif "max" in product_name:
        version = "max"
    elif "plus" in product_name:
        version = "plus"
    
    cap_match = re.search(r'(\d{2,4})(gb|tb)', product_name)
    if cap_match:
        capacity = cap_match.group(1)
        if cap_match.group(2) == "tb":
            capacity = str(int(capacity) * 1000)
    else:
        capacity = "cap"
    
    return f"ip{gen}{version}{capacity}"

def create_ids(df):
    df["SellerCode"] = df["Seller"].str.lower().str.replace(r'\W+', '', regex=True)
    df["ModelCode"] = df["Product"].apply(generate_model_code)
    df["StateCode"] = df["State"]
    
    df["ReadableID"] = ("cdc_" + df["SellerCode"] + "_" + 
                       df["ModelCode"] + df["StateCode"])
    
    df["NumericID"] = df["ReadableID"].apply(
    lambda x: int(hashlib.md5(x.encode()).hexdigest()[:8], 16))
    
    return df

def clean_data(df):
    df = df.sort_values(["ReadableID", "Timestamp", "Price"])
    df = df.drop_duplicates(subset=["ReadableID", "Timestamp"], keep="first")
    
    price_stats = df.groupby("ReadableID")["Price"].agg(['mean', 'std'])
    df = df.merge(price_stats, on="ReadableID", how="left")
    df = df[(df["Price"] - df["mean"]).abs() <= 3 * df["std"]]
    
    return df.drop(columns=['mean', 'std'])

def detect_pricing_strategy(df):
    changes = df.groupby("ReadableID").agg(
        PriceChanges=("Price", "nunique"),
        TimeChanges=("Day", "nunique"),
        MeanPrice=("Price", "mean"),
        MedianPrice=("Price", "median")
    ).reset_index()
    
    changes["Algorithmic"] = changes["PriceChanges"] >= 6
    return df.merge(changes, on="ReadableID", how="left")

def generate_visualizations(df):
    if df.empty:
        return
    
    if df["ModelCode"].nunique() > 1:
        plt.figure(figsize=(14, 8))
        top_models = df["ModelCode"].value_counts().nlargest(5).index
        sns.boxplot(data=df[df["ModelCode"].isin(top_models)],
                   x="ModelCode", y="Price", hue="State")
        plt.title("Distribution des prix par modèle (Cdiscount)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "price_distribution.png"))
        plt.close()
    
    if df["Day"].nunique() > 1:
        plt.figure(figsize=(12, 6))
        df.groupby("Day").size().plot(kind="bar")
        plt.title("Nombre d'observations par jour (Cdiscount)")
        plt.savefig(os.path.join(FIG_DIR, "observations_per_day.png"))
        plt.close()

def prepare_for_modeling(df):
    df = df.sort_values(["ReadableID", "Timestamp"])
    
    df["PriceChange"] = df.groupby("ReadableID")["Price"].diff()
    df["DaysSinceLastChange"] = df.groupby("ReadableID")["Timestamp"].diff().dt.days
    
    model_avg = df.groupby(["ModelCode", "Day"])["Price"].mean().rename("ModelAvgPrice")
    df = df.merge(model_avg, on=["ModelCode", "Day"], how="left")
    
    return df

def main():
    print("Chargement données")
    df = load_and_clean_data(CSV_PATH)
    
    if df.empty:
        print("Arrêt: Aucune donnée valide à traiter")
        return
    
    print(f"Données chargées: {len(df)} lignes")
    
    print("Création id")
    df = create_ids(df)
    
    print("Nettoyage données")
    df = clean_data(df)
    
    print("Détection strats")
    df = detect_pricing_strategy(df)
    
    print("Génération graphs")
    generate_visualizations(df)
    
    print("Préparation model")
    df = prepare_for_modeling(df)
    
    df["CodeID"] = df["NumericID"]
    df = df[["Timestamp", "Day", "ModelCode", "CodeID", "ReadableID", "Price", "Rating", "Algorithmic", "PriceChanges", "TimeChanges"]]
    df = df.rename(columns={
        "Algorithmic": "algo_suspect",
        "PriceChanges": "n_prices",
        "TimeChanges": "n_dates"
    })
    
    print("Export résultats")
    try:
        df.to_csv(EXPORT_PATH, index=False)
        print(f"Données sauvegardées dans {EXPORT_PATH}")
    except Exception as e:
        print(f"Erreur export: {str(e)}")
    
    print("\nCCL")
    print(f"- {len(df)} observations valides")
    print(f"- {df['ReadableID'].nunique()} produits uniques")
    print(f"- {df['algo_suspect'].sum()} suspects")

if __name__ == "__main__":
    main()