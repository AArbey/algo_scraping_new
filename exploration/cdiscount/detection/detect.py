import pandas as pd
from datetime import datetime
import seaborn as sns
import os
import matplotlib.pyplot as plt

# Configuration
X = 20  # Seuil de changements de prix par jour à détecter
DATA_PATH = "/home/scraping/algo_scraping/exploration/cdiscount/data_cleaned_cdiscount.csv"
OUTPUT_DIR = "/home/scraping/algo_scraping/exploration/cdiscount/detection/algo_detection1_simple"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def detect_high_frequency_sellers(df, changes_threshold):
    df['Date'] = df['Timestamp'].dt.date
    
    df['PriceChange'] = df.groupby('ReadableID')['Price'].diff().ne(0)
    daily_changes = df.groupby(['ReadableID', 'Date'])['PriceChange'].sum().reset_index()
    
    high_freq_sellers = daily_changes[daily_changes['PriceChange'] > changes_threshold]
    
    seller_stats = daily_changes.groupby('ReadableID').agg(
        AvgDailyChanges=('PriceChange', 'mean'),
        DaysActive=('Date', 'nunique')
    ).reset_index()
    
    results = high_freq_sellers.merge(seller_stats, on='ReadableID')
    return results.sort_values(['PriceChange', 'AvgDailyChanges'], ascending=False)


def generate_supplier_visualizations(df, high_freq_sellers, output_dir):
    df['SellerName'] = df['ReadableID'].str.extract(r'cdc_(.*?)_')[0]
    df['Model'] = df['ModelCode'].str.extract(r'(ip\d{2})')[0]
    
    high_freq_df = df[df['ReadableID'].isin(high_freq_sellers['ReadableID'])]
    
    plt.figure(figsize=(14, 8))
    sns.boxplot(
        data=high_freq_df,
        x='SellerName',
        y='Price',
        hue='Model'
    )
    plt.title(f"Distribution des prix par fournisseur (>{X} changements/jour)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "price_distribution_by_supplier.png"))
    plt.close()
    
    plt.figure(figsize=(14, 8))
    sns.boxplot(
        data=high_freq_df,
        x='Model',
        y='Price',
        hue='SellerName'
    )
    plt.title(f"Distribution des prix par modèle (>{X} changements/jour)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "price_distribution_by_model.png"))
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
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Chargement des données...")
    try:
        df = pd.read_csv(DATA_PATH, parse_dates=["Timestamp"])
    except FileNotFoundError:
        print(f"Erreur : Fichier introuvable à {DATA_PATH}")
        return
    
    print(f"Analyse avec seuil = {X} changements/jour...")
    results = detect_high_frequency_sellers(df, X)
    

    plot_daily_price_changes(df, OUTPUT_DIR, "seuil_"+str(X))
    generate_supplier_visualizations(df, results, OUTPUT_DIR)
    
    output_path = os.path.join(OUTPUT_DIR, f"sellers_above_{X}_changes_per_day.csv")
    results.to_csv(output_path, index=False)
    
    print(f"\nVendeurs détectés (> {X} changements/jour) : {len(results['ReadableID'].unique())}")
    print(f"Exemples :")
    print(results.head(10)[['ReadableID', 'Date', 'PriceChange', 'AvgDailyChanges']])
    print(f"\nRésultats complets sauvegardés dans : {output_path}")

if __name__ == "__main__":
    main()