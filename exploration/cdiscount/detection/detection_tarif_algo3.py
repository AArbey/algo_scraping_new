import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

X = 20  # Seuil de changements de prix par jour
DATA_PATH = "/home/scraping/algo_scraping/exploration/cdiscount/data_cleaned_cdiscount.csv"
OUTPUT_DIR = "/home/scraping/algo_scraping/exploration/cdiscount/detection/algo_detection3_combined"
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
    return df[df["ReadableID"].isin(valid_sellers)]

def detect_high_frequency_sellers(df, threshold):
    df['Date'] = df['Timestamp'].dt.date
    df['PriceChange'] = df.groupby('ReadableID')['Price'].diff().ne(0)
    daily_changes = df.groupby(['ReadableID', 'Date'])['PriceChange'].sum().reset_index()
    return daily_changes[daily_changes['PriceChange'] > threshold]['ReadableID'].unique()

def calculate_algo_metrics(df):
    metrics = []
    for seller, group in df.groupby("ReadableID"):
        group = group.sort_values("Timestamp").copy()
        price_changes = group["Price"].diff().ne(0)
        time_diffs = group["Timestamp"].diff().dt.total_seconds().dropna()
        
        metrics.append({
            "ReadableID": seller,
            "ChangeFrequency": price_changes.mean(),
            "CompetitorResponse": group[["Price", "Price_mean_competitor"]].dropna().corr().iloc[0,1] if 'Price_mean_competitor' in group.columns else 0,
            "TimeRegularity": 1/(time_diffs.std() + 1e-6) if time_diffs.std() > 0 else 0,
            "ObsCount": len(group)
        })
    return pd.DataFrame(metrics)

def detect_algo_sellers(metrics_df):
    features = metrics_df[["ChangeFrequency", "CompetitorResponse", "TimeRegularity"]]
    X_scaled = StandardScaler().fit_transform(SimpleImputer(strategy='mean').fit_transform(features))
    
    kmeans = KMeans(n_clusters=2, random_state=42)
    clusters = kmeans.fit_predict(X_scaled)
    algo_cluster = pd.DataFrame(X_scaled).groupby(clusters).mean().mean(axis=1).idxmax()
    
    metrics_df["AlgoCluster"] = clusters == algo_cluster
    metrics_df["ClusterScore"] = kmeans.transform(X_scaled).min(axis=1)
    return metrics_df


def generate_enhanced_visualizations(df, combined_sellers, metrics_df, output_dir):
    df['SellerName'] = df['ReadableID'].str.extract(r'cdc_(.*?)_')[0]
    df['Model'] = df['ModelCode'].str.extract(r'(ip\d{2})')[0]
    
    combined_df = df[df['ReadableID'].isin(combined_sellers)]
    
    plt.figure(figsize=(14, 8))
    ax = sns.boxplot(
        data=combined_df,
        x='Model',
        y='Price',
        hue='SellerName'
    )
    plt.title(f"Distribution des prix par modèle (Vendeurs algorithmiques, >{X} changements/jour)")
    plt.xticks(rotation=45, ha='right')
    
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "algo_price_distribution_by_model.png"), 
                bbox_inches='tight')
    plt.close()
    
    plt.figure(figsize=(14, 8))
    ax = sns.boxplot(
        data=combined_df,
        x='SellerName',
        y='Price',
        hue='Model'
    )
    plt.title(f"Distribution des prix par fournisseur (Vendeurs algorithmiques, >{X} changements/jour)")
    plt.xticks(rotation=45, ha='right')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "algo_price_distribution_by_seller.png"),
                bbox_inches='tight')
    plt.close()
    
    plt.figure(figsize=(12, 8))
    ax = sns.scatterplot(
        data=metrics_df,
        x="ChangeFrequency",
        y="CompetitorResponse",
        hue=metrics_df["ReadableID"].isin(combined_sellers),
        style="AlgoCluster",
        size="ObsCount",
        alpha=0.7,
        palette={True: "red", False: "blue"}
    )
    plt.title(f"Vendeurs Algorithmiques (Intersection X>{X} changements/jour)")
    plt.xlabel("Fréquence des Changements de Prix")
    plt.ylabel("Réponse aux Concurrents (Corrélation)")
    
    handles, labels = ax.get_legend_handles_labels()
    plt.legend(handles=handles, 
                labels=labels,
                bbox_to_anchor=(1.05, 1), 
                loc='upper left')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "enhanced_combined_detection.png"),
                bbox_inches='tight')
    plt.close()

def plot_daily_price_changes(df, output_dir, title_suffix=""):
    df['Date'] = df['Timestamp'].dt.date
    df['PriceChange'] = df.groupby('ReadableID')['Price'].diff().ne(0).astype(int)
    daily_changes = df.groupby(['ReadableID', 'Date'])['PriceChange'].sum().reset_index()
    
    plt.figure(figsize=(14, 8))
    
    boxprops = dict(linestyle='-', linewidth=1.5, color='darkblue')
    medianprops = dict(linestyle='-', linewidth=2.5, color='firebrick')
    flierprops = dict(marker='o', markersize=5, markerfacecolor='none', markeredgecolor='gray')
    
    sns.boxplot(
        data=daily_changes,
        x='ReadableID',
        y='PriceChange',
        color='lightblue',
        showfliers=True,
        boxprops=boxprops,
        medianprops=medianprops,
        flierprops=flierprops
    )
    
    plt.title(f"Distribution quotidienne des changements de prix par vendeur {title_suffix}", pad=20)
    plt.xlabel("Vendeur")
    plt.ylabel("Nombre de changements de prix par jour")
    plt.xticks(rotation=90)
    plt.grid(axis='y', alpha=0.4)
    
    plt.tight_layout()
    
    filename = f"daily_price_changes_boxplot{'_' + title_suffix if title_suffix else ''}.png"
    plt.savefig(os.path.join(output_dir, filename), dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Boxplot des changements quotidiens sauvegardé sous {filename}")


def main():
    print(f"{datetime.now()} - Chargement des données...")
    df = load_and_prepare_data(DATA_PATH)
    
    print(f"Détection des vendeurs avec >{X} changements/jour...")
    high_freq_sellers = detect_high_frequency_sellers(df, X)
    
    print("Calcul des métriques algorithmiques...")
    metrics_df = calculate_algo_metrics(df)
    metrics_df = detect_algo_sellers(metrics_df)
    algo_sellers = metrics_df[metrics_df["AlgoCluster"]]["ReadableID"].unique()
    
    combined_sellers = set(high_freq_sellers) & set(algo_sellers)
    print(f"\nVendeurs détectés par les deux méthodes (intersection): {len(combined_sellers)}")
    
    results = metrics_df[metrics_df["ReadableID"].isin(combined_sellers)].sort_values("ClusterScore")
    results["HighFrequencyChanges"] = results["ReadableID"].apply(lambda x: x in high_freq_sellers)
    
    output_path = os.path.join(OUTPUT_DIR, f"combined_algo_sellers_threshold_{X}.csv")
    results.to_csv(output_path, index=False)
    print(f"Résultats sauvegardés dans {output_path}")
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=metrics_df,
        x="ChangeFrequency",
        y="CompetitorResponse",
        hue=metrics_df["ReadableID"].isin(combined_sellers),
        style="AlgoCluster",
        alpha=0.7
    )
    plt.title(f"Vendeurs Algorithmiques (Intersection X>{X} changements/jour)")
    plt.savefig(os.path.join(OUTPUT_DIR, "combined_detection.png"))
    plt.close()

    print("Génération des visualisations améliorées...")
    
    plot_daily_price_changes(df, OUTPUT_DIR, "seuil_"+str(X))
    generate_enhanced_visualizations(df, combined_sellers, metrics_df, OUTPUT_DIR)
    print(f"Visualisations sauvegardées dans {OUTPUT_DIR}")

if __name__ == "__main__":
    main()