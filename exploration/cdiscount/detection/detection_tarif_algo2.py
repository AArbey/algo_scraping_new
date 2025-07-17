import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

DATA_PATH = "/home/scraping/algo_scraping/exploration/cdiscount/data_cleaned_cdiscount.csv"
OUTPUT_DIR = "/home/scraping/algo_scraping/exploration/cdiscount/detection/algo_detection2"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_and_prepare_data(path):
    df = pd.read_csv(path, parse_dates=["Timestamp"])
    df = df.sort_values(["ReadableID", "Timestamp"])
    
    if 'Price_mean_competitor' not in df.columns:
        print("Calcul du prix moyen des concurrents...")
        df["Price_mean_competitor"] = df.groupby(["ModelCode", pd.Grouper(key="Timestamp", freq="h")])["Price"].transform('mean')
        df["Price_mean_competitor"] = df["Price_mean_competitor"] - df["Price"]
    
    seller_counts = df["ReadableID"].value_counts()
    valid_sellers = seller_counts[seller_counts >= 5].index
    df = df[df["ReadableID"].isin(valid_sellers)]
    
    return df

def calculate_algo_metrics(df):
    metrics = []
    
    for seller, group in df.groupby("ReadableID"):
        group = group.sort_values("Timestamp").copy()
        
        price_changes = group["Price"].diff().ne(0)
        change_freq = price_changes.mean()
        
        price_diffs = group["Price"].diff().abs()
        avg_change_size = price_diffs.mean()
        
        comp_response = 0
        if 'Price_mean_competitor' in group.columns:
            valid_points = group[["Price", "Price_mean_competitor"]].dropna()
            if len(valid_points) > 1:
                comp_response = valid_points["Price"].corr(valid_points["Price_mean_competitor"])
        
        time_diffs = group["Timestamp"].diff().dt.total_seconds().dropna()
        time_std = time_diffs.std()
        time_regularity = 1/(time_std + 1e-6) if time_std > 0 else 0
        
        last_digits = (group["Price"] * 100 % 100)
        psych_pricing = ((last_digits >= 95) | (last_digits <= 5)).mean()
        
        metrics.append({
            "ReadableID": seller,
            "ChangeFrequency": change_freq if not np.isnan(change_freq) else 0,
            "AvgChangeSize": avg_change_size if not np.isnan(avg_change_size) else 0,
            "CompetitorResponse": comp_response if not np.isnan(comp_response) else 0,
            "TimeRegularity": time_regularity,
            "PsychologicalPricing": psych_pricing,
            "ObsCount": len(group),
            "FirstDate": group["Timestamp"].min(),
            "LastDate": group["Timestamp"].max()
        })
    
    return pd.DataFrame(metrics)

def detect_algo_sellers(metrics_df):
    features = metrics_df[["ChangeFrequency", "CompetitorResponse", "TimeRegularity"]]
    
    imputer = SimpleImputer(strategy='mean')
    X_imputed = imputer.fit_transform(features)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_imputed)
    
    kmeans = KMeans(n_clusters=2, random_state=42)
    clusters = kmeans.fit_predict(X_scaled)
    
    cluster_means = pd.DataFrame(X_scaled).groupby(clusters).mean()
    algo_cluster = cluster_means.mean(axis=1).idxmax()
    
    metrics_df["AlgoCluster"] = clusters == algo_cluster
    metrics_df["ClusterScore"] = kmeans.transform(X_scaled).min(axis=1)
    
    return metrics_df, kmeans

def visualize_results(metrics_df, output_dir):
    plt.figure(figsize=(12, 8))
    sns.scatterplot(
        data=metrics_df,
        x="ChangeFrequency",
        y="CompetitorResponse",
        hue="AlgoCluster",
        style="AlgoCluster",
        size="ObsCount",
        alpha=0.7
    )
    plt.title("Détection des Sellers Algorithmiques")
    plt.xlabel("Fréquence des Changements de Prix")
    plt.ylabel("Réponse aux Concurrents (Corrélation)")
    plt.savefig(os.path.join(output_dir, "algo_detection_scatter.png"))
    plt.close()
    
    for metric in ["ChangeFrequency", "CompetitorResponse", "TimeRegularity"]:
        plt.figure(figsize=(10, 6))
