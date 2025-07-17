import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
import os
import matplotlib.dates as mdates
from datetime import timedelta

# Configuration des chemins
FIGURES_DIR = "/home/scraping/algo_scraping/exploration/cdiscount/figures_pred/"
os.makedirs(FIGURES_DIR, exist_ok=True)

# Chargement robuste des données
try:
    df = pd.read_csv(
        "/home/scraping/algo_scraping/exploration/cdiscount/NEWdata_enriched_cdiscount.csv",
        parse_dates=["Timestamp"]
    )
except Exception as e:
    print(f"Erreur de chargement : {str(e)}")
    exit()

# Vérification des colonnes requises
required_cols = ["ReadableID", "Price", "Timestamp", "mean_3_closest", "median_price"]
if not all(col in df.columns for col in required_cols):
    print(f"Colonnes manquantes. Requises: {required_cols}")
    print(f"Disponibles: {df.columns.tolist()}")
    exit()

# Fonction d'analyse principale
def run_base_analysis():
    """Analyse statistique de base"""
    results = []
    features = ["mean_3_closest", "median_price", "mean_2_max", "mean_5_min", "mean_3_min"]
    
    plt.figure(figsize=(15, 8))
    sns.set_style("whitegrid")
    
    for i, var in enumerate(features):
        df_clean = df[["Price", var]].dropna()
        if len(df_clean) < 2:
            continue
            
        X = sm.add_constant(df_clean[var])
        y = df_clean["Price"]
        model = sm.OLS(y, X).fit()
        
        results.append({
            "Variable": var,
            "Coef": round(model.params[var], 4),
            "R²": round(model.rsquared, 4),
            "MAE": round(np.mean(np.abs(y - model.predict(X))), 2)
        })
        
        # Graphique
        plt.subplot(2, 3, i+1)
        sns.regplot(x=var, y='Price', data=df_clean, 
                    scatter_kws={'alpha':0.3}, line_kws={'color':'red'})
        plt.title(f"{var}\nR²={model.rsquared:.2f}")
    
    plt.tight_layout()
    plt.savefig(f"{FIGURES_DIR}/price_analysis.png", dpi=120)
    plt.close()
    
    return pd.DataFrame(results).sort_values("R²", ascending=False)

# Fonction de prédiction spécifique
def plot_phone_predictions(phone_id, days_to_predict=3):
    """Visualisation pour un téléphone spécifique"""
    # Préparation des données
    phone_data = df[df["ReadableID"] == phone_id].copy()
    
    if phone_data.empty:
        print(f"\nErreur : ID '{phone_id}' introuvable.")
        print("IDs disponibles (5 exemples):")
        print(df["ReadableID"].drop_duplicates().sample(5).to_string(index=False))
        return
    
    phone_data = phone_data.sort_values("Timestamp").dropna(
        subset=["Timestamp", "Price", "mean_3_closest", "median_price"]
    )
    
    if len(phone_data) < 2:
        print(f"Données insuffisantes pour {phone_id} (minimum 2 observations)")
        return
    
    # Modélisation avec les 2 meilleures variables
    X = phone_data[["mean_3_closest", "median_price"]]
    X = sm.add_constant(X)
    y = phone_data["Price"]
    
    try:
        model = sm.OLS(y, X).fit()
    except Exception as e:
        print(f"Erreur de modélisation : {str(e)}")
        return
    
    # Prédictions
    phone_data["Predicted"] = model.predict(X)
    
    # Visualisation
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Données historiques
    ax.scatter(phone_data["Timestamp"], phone_data["Price"], 
               color='blue', alpha=0.7, label='Prix réels')
    ax.plot(phone_data["Timestamp"], phone_data["Predicted"], 
            color='red', linewidth=2, label='Modèle')
    
    # Projection future
    if days_to_predict > 0:
        last_data = phone_data.iloc[-1]
        future_dates = [last_data["Timestamp"] + timedelta(days=i) for i in range(1, days_to_predict+1)]
        
        future_X = pd.DataFrame({
            "const": [1]*days_to_predict,
            "mean_3_closest": [last_data["mean_3_closest"]]*days_to_predict,
            "median_price": [last_data["median_price"]]*days_to_predict
        })
        
        try:
            future_pred = model.predict(future_X[["const", "mean_3_closest", "median_price"]])
            ax.plot(future_dates, future_pred, 'r--', 
                    linewidth=2, label=f'Projection ({days_to_predict}j)')
        except Exception as e:
            print(f"Erreur de projection : {str(e)}")
    
    # Mise en forme
    ax.set_title(f"Analyse de prix - {phone_id}", pad=20)
    ax.set_xlabel("Date")
    ax.set_ylabel("Prix (€)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
    ax.grid(alpha=0.3)
    ax.legend()
    
    plt.tight_layout()
    
    # Sauvegarde
    safe_name = "".join(c for c in phone_id if c.isalnum() or c in ('_', '-'))[:50]
    plt.savefig(f"{FIGURES_DIR}/pred_{safe_name}.png", dpi=120, bbox_inches='tight')
    plt.close()
    print(f"Graphique généré : pred_{safe_name}.png")

# Exécution principale
if __name__ == "__main__":
    print("Début de l'analyse...")
    
    # 1. Analyse globale
    print("\nAnalyse statistique des variables :")
    results = run_base_analysis()
    print(results.to_string(index=False))
    
    # 2. Boucle sur un fichier d'IDs
    ids_path = "/home/scraping/algo_scraping/exploration/cdiscount/detection/algo_detection1_simple/sellers_above_20_changes_per_day.csv"
    try:
        ids_df = pd.read_csv(ids_path)
        ids_list = ids_df["ReadableID"].drop_duplicates().tolist()
    except Exception as e:
        print(f"Erreur de lecture du fichier d'IDs : {str(e)}")
        ids_list = []

    for target_id in ids_list:
        print(f"\nAnalyse pour l'ID : {target_id}")
        plot_phone_predictions(target_id, days_to_predict=3)

    print("\nAnalyse terminée avec succès!")

    """
    # 2. Analyse spécifique
    target_id = "cdc_alloccaz_ip14promax128er"  # Exemple à modifier
    print(f"\nAnalyse pour l'ID : {target_id}")
    plot_phone_predictions(target_id, days_to_predict=3)
    
    print("\nAnalyse terminée avec succès!")
    """