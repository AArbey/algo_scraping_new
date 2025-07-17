import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re

CSV_PATH = "/home/scraping/algo_scraping/RAKUTEN/Rakuten_data.csv"
EXPORT_PATH = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_cleaned_rakuten_lag.csv"
FIG_DIR = "/home/scraping/algo_scraping/exploration/rakuten/figures"
os.makedirs(FIG_DIR, exist_ok=True)

csv_file = CSV_PATH
excel_file = '/home/scraping/algo_scraping/ID_EXCEL.xlsx'

columns = [
    "pfid", "idsmartphone", "url", "timestamp", "price", "shipcost", 
    "rating", "ratingnb", "offertype", "offerdetails", 
    "shipcountry", "sellercountry", "seller"
]

TOP_N_MODELS = 6

def extract_model_code(product_name, state):
    name = product_name.lower()
    name = name.replace("apple", "")
    name = name.replace("iphone", "ip")
    name = name.replace("+", " plus")

    gen_match = re.search(r'ip\s?(\d{2})', name)
    gen = gen_match.group(1) if gen_match else "unk"

    version = ""
    if re.search(r'pro[\s\-]?max', name):
        version = "promax"
    elif re.search(r'\bpro\b', name):
        version = "pro"
    elif re.search(r'\bmax\b', name):
        version = "max"
    elif re.search(r'\bplus\b', name):
        version = "plus"

    cap_match = re.search(r'(\d{2,4})\s?(go|to|gb)', name)
    if cap_match:
        capacity = cap_match.group(1)
        if cap_match.group(2) == "to":
            try:
                capacity = str(int(capacity) * 1000)
            except:
                capacity = "unk"
    else:
        capacity = "unk"

    suffix = state if state in ["n", "u"] else "unk"
    model_code = f"ip{gen}{version}{capacity}{suffix}"
    model_code = re.sub(r'(unk)+', 'unk', model_code)
    return model_code

def load_smartphone_models_from_excel():
    try:
        df_excel = pd.read_excel(excel_file, skiprows=7, header=None, dtype=str)
        if df_excel.shape[1] >= 3:
            df_excel = df_excel.iloc[:, 1:3]
            df_excel.columns = ['Phone', 'idsmartphone']
            return df_excel.dropna()
        else:
            print("Le fichier Excel ne contient pas suffisamment de colonnes.")
            return pd.DataFrame(columns=['Phone', 'idsmartphone'])
    except Exception as e:
        print(f"Erreur lecture Excel : {e}")
        return pd.DataFrame(columns=['Phone', 'idsmartphone'])

def load_and_clean_rakuten_data():
    try:
        df = pd.read_csv(csv_file, sep=",", engine='python', on_bad_lines='skip')
        if df.empty:
            print("Fichier CSV vide.")
            return pd.DataFrame(columns=columns)

        df['price'] = df['price'].astype(str).str.replace('€', '').str.replace(',', '.').str.strip()
        df = df[df['price'].notnull()]
        df = df[df['price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
        df['price'] = df['price'].astype(float)
        df = df[df['price'] <= 3000]

        df['shipcost'] = df['shipcost'].fillna(0).astype(float)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y/%m/%d %H:%M', errors='coerce')
        df = df[df['timestamp'].notnull()]
        df['Rounded_Timestamp'] = df['timestamp'].dt.round('1min')
        df['seller'] = df['seller'].fillna("Unknown")

        smartphone_models = load_smartphone_models_from_excel()
        if not smartphone_models.empty:
            df = df.merge(smartphone_models, on='idsmartphone', how='left')
            df['idsmartphone'] = df['Phone']
            df.drop(columns=['Phone'], inplace=True)

        top_ids = df['idsmartphone'].value_counts().nlargest(TOP_N_MODELS).index
        df = df[df['idsmartphone'].isin(top_ids)]

        return df
    except Exception as e:
        print(f"Erreur chargement données Rakuten : {e}")
        return pd.DataFrame(columns=columns)

def load_and_clean_data(path):
    df = load_and_clean_rakuten_data()
    df["Price"] = df["price"]
    df["Timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", dayfirst=False)
    df = df[df["Timestamp"].notnull()]
    df["Rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["Seller"] = df["seller"]
    df["Day"] = df["Timestamp"].dt.floor("D")
    return df

def create_ids(df):
    df["PlatformCode"] = "rak"
    df["SellerCode"] = df["Seller"].str.lower().str.replace(r'\W+', '', regex=True)
    df["StateCode"] = df["offertype"].fillna("").str.lower().map(lambda x: "n" if "new" in x else "u")
    df["ModelCode"] = df.apply(lambda row: extract_model_code(row["idsmartphone"], row["StateCode"]) if isinstance(row["idsmartphone"], str) else "ipunkunk", axis=1)
    df["ReadableID"] = df["PlatformCode"] + "_" + df["SellerCode"] + "_" + df["ModelCode"]
    return df


def add_algo_flags(df, seuil_journalier=20):
    """
    Flagge comme suspects les vendeurs qui dépassent un seuil donné de
    changements de prix successifs dans une même journée.
    Ajoute aussi n_true_changes_day au niveau ligne.
    """
    df_sorted = df.sort_values(["ReadableID", "Day", "Timestamp"]).copy()
    df_sorted["PrevPrice"] = df_sorted.groupby(["ReadableID", "Day"])["Price"].shift()
    df_sorted["PriceChanged"] = df_sorted["Price"] != df_sorted["PrevPrice"]

    daily_changes = (
        df_sorted.groupby(["ReadableID", "Day"])["PriceChanged"]
        .sum()
        .reset_index()
        .rename(columns={"PriceChanged": "n_true_changes_day"})
    )

    # Ajout dans le DataFrame ligne par ligne
    df = df.merge(daily_changes, on=["ReadableID", "Day"], how="left")

    # Détection des vendeurs suspects (ceux qui dépassent le seuil au moins un jour)
    max_changes = (
        daily_changes.groupby("ReadableID")["n_true_changes_day"]
        .max()
        .reset_index()
    )
    max_changes["algo_suspect"] = max_changes["n_true_changes_day"] > seuil_journalier

    # Fusion avec algo_suspect
    df = df.merge(max_changes[["ReadableID", "algo_suspect"]], on="ReadableID", how="left")
    df["algo_suspect"] = df["algo_suspect"].fillna(False)

    return df



def generate_structured_numeric_id(df):
    df["Platform"] = "Rakuten"
    platform_map = {name: i for i, name in enumerate(sorted(df["Platform"].unique()))}
    seller_map = {name: i for i, name in enumerate(sorted(df["Seller"].dropna().astype(str).str.lower().unique()))}
    model_map = {name: i for i, name in enumerate(sorted(df["ModelCode"].unique()))}

    df["PlatformCodeNum"] = df["Platform"].map(platform_map)
    df["SellerCodeNum"] = df["Seller"].str.lower().map(seller_map)
    df["ModelCodeNum"] = df["ModelCode"].map(model_map)

    df["CodeID"] = (
        df["PlatformCodeNum"] * 1_000_000 +
        df["SellerCodeNum"] * 1_000 +
        df["ModelCodeNum"]
    )
    return df


def plot_true_price_changes_boxplot(df, model_code):
    model_data = df[df["ModelCode"] == model_code].copy()
    if model_data.empty:
        print(f"Aucune donnée pour le modèle {model_code}")
        return

    # Trier pour détecter changements successifs
    model_data = model_data.sort_values(["Seller", "Day", "Timestamp"])
    model_data["PrevPrice"] = model_data.groupby(["Seller", "Day"])["Price"].shift()
    model_data["PriceChanged"] = model_data["Price"] != model_data["PrevPrice"]

    # Compter les changements par vendeur et jour
    change_counts = model_data.groupby(["Seller", "Day"])["PriceChanged"].sum().reset_index()
    change_counts.rename(columns={"PriceChanged": "n_price_changes"}, inplace=True)

    # Tracer
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=change_counts, x="Seller", y="n_price_changes")
    plt.title(f"Changements de prix réels par jour – modèle {model_code}")
    plt.ylabel("Nombre de changements dans la journée")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"2boxplot_true_changes_{model_code}.png"))
    plt.close()


def plot_true_price_changes_boxplot_for_top_models(df):
    top_n=5
    top_models = df["ModelCode"].value_counts().nlargest(top_n).index
    for model_code in top_models:
        plot_true_price_changes_boxplot(df, model_code)

def plot_cdf_true_price_changes(df):
    if "ReadableID" not in df.columns or "Timestamp" not in df.columns or "Price" not in df.columns:
        print("Colonnes nécessaires absentes.")
        return

    # Trier dans l'ordre temporel
    df_sorted = df.sort_values(["ReadableID", "Timestamp"]).copy()
    df_sorted["PrevPrice"] = df_sorted.groupby("ReadableID")["Price"].shift()
    df_sorted["PriceChanged"] = df_sorted["Price"] != df_sorted["PrevPrice"]

    # Compter les vrais changements successifs par ReadableID
    change_counts = df_sorted.groupby("ReadableID")["PriceChanged"].sum().reset_index()
    change_counts.rename(columns={"PriceChanged": "n_true_price_changes"}, inplace=True)

    # Calcul de la CDF
    sorted_counts = np.sort(change_counts["n_true_price_changes"])
    cdf = np.arange(1, len(sorted_counts) + 1) / len(sorted_counts)

    # Tracé
    plt.figure(figsize=(10, 6))
    plt.plot(sorted_counts, cdf, marker=".", linestyle="-")
    plt.xlabel("Nombre réel de changements de prix (successifs) par vendeur-produit")
    plt.ylabel("Fonction de répartition cumulée (CDF)")
    plt.title("CDF des vrais changements de prix successifs (Rakuten)")
    plt.grid(True)
    plt.tight_layout()
    save_path = os.path.join(FIG_DIR, "cdf_true_price_changes_rakuten.png")
    plt.savefig(save_path)
    plt.close()
    print(f"CDF (vrais changements) sauvegardée dans {save_path}")

def plot_cdf_daily_price_changes(df):
    """
    Calcule et trace la CDF du nombre de vrais changements de prix successifs
    par jour et par vendeur-produit (ReadableID).
    """
    required_cols = {"ReadableID", "Timestamp", "Price", "Day"}
    if not required_cols.issubset(df.columns):
        print(f"Colonnes manquantes : {required_cols - set(df.columns)}")
        return

    # Tri temporel
    df_sorted = df.sort_values(["ReadableID", "Day", "Timestamp"]).copy()
    
    # Détection de changements successifs par jour
    df_sorted["PrevPrice"] = df_sorted.groupby(["ReadableID", "Day"])["Price"].shift()
    df_sorted["PriceChanged"] = df_sorted["Price"] != df_sorted["PrevPrice"]

    # Nombre de changements par ReadableID et jour
    daily_changes = (
        df_sorted.groupby(["ReadableID", "Day"])["PriceChanged"]
        .sum()
        .reset_index()
        .rename(columns={"PriceChanged": "n_true_changes_day"})
    )

    # Tri et calcul de la CDF
    sorted_counts = np.sort(daily_changes["n_true_changes_day"])
    cdf = np.arange(1, len(sorted_counts) + 1) / len(sorted_counts)

    # Tracé
    plt.figure(figsize=(10, 6))
    plt.plot(sorted_counts, cdf, marker=".", linestyle="-")
    plt.xlabel("Nombre réel de changements de prix dans la journée (par ReadableID)")
    plt.ylabel("Fonction de répartition cumulée (CDF)")
    plt.title("CDF des changements de prix par jour – Rakuten")
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(FIG_DIR, "cdf_daily_true_price_changes.png")
    plt.savefig(save_path)
    plt.close()
    print(f"CDF journalière sauvegardée dans {save_path}")




def main():
    df = load_and_clean_data(CSV_PATH)
    df = create_ids(df)
    df = generate_structured_numeric_id(df)
    df = add_algo_flags(df)
    df = df[["Timestamp", "Seller", "CodeID", "ReadableID", 
             "Price","n_true_changes_day", "algo_suspect"]].copy()

    #df.to_csv(EXPORT_PATH, index=False)
    #plot_true_price_changes_boxplot_for_top_models(df)


    print(f"Fichier exporté : {EXPORT_PATH}")
    n_sellers_suspects = df[df["algo_suspect"]]["Seller"].nunique()
    n_total_sellers = df["Seller"].nunique()
    print(f"{n_sellers_suspects} vendeurs suspects sur {n_total_sellers} vendeurs uniques ({100 * n_sellers_suspects / n_total_sellers:.1f}%)")

if __name__ == "__main__":
    main()
