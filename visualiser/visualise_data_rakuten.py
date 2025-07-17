# Dash visualisation pour les offres Rakuten

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

# Chemin vers le fichier CSV et le fichier Excel contenant les descriptions
csv_file = '/home/scraping/algo_scraping/RAKUTEN/Rakuten_data.csv'
excel_file = '/home/scraping/algo_scraping/ID_EXCEL.xlsx'

columns = [
    "pfid", "idsmartphone", "url", "timestamp", "price", "shipcost", 
    "rating", "ratingnb", "offertype", "offerdetails", 
    "shipcountry", "sellercountry", "seller"
]

TOP_N_MODELS = 6  # Nombre de modèles les plus présents à afficher

def load_smartphone_models_from_excel():
    try:
        df_excel = pd.read_excel(excel_file, skiprows=7, header=None, dtype=str)
        if df_excel.shape[1] >= 3:
            df_excel = df_excel.iloc[:, 1:3]  # Colonnes B et C
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

        # Exclude rows where the price is above 3000€
        df = df[df['price'] <= 3000]

        df['shipcost'] = df['shipcost'].fillna(0).astype(float)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y/%m/%d %H:%M', errors='coerce')
        df = df[df['timestamp'].notnull()]
        df['Rounded_Timestamp'] = df['timestamp'].dt.round('5min')
        df['seller'] = df['seller'].fillna("Unknown")

        smartphone_models = load_smartphone_models_from_excel()
        if smartphone_models.empty:
            print("Aucun modèle de smartphone valide trouvé.")
        else:
            df = df.merge(smartphone_models, on='idsmartphone', how='left')
            df['idsmartphone'] = df['Phone']
            df.drop(columns=['Phone'], inplace=True)
            # Garder le prix minimal par modèle, vendeur et timestamp arrondi
            df = df.sort_values('price').drop_duplicates(subset=['idsmartphone', 'seller', 'Rounded_Timestamp'], keep='first')

        # Réduire à TOP_N_MODELS les plus fréquents
        top_ids = df['idsmartphone'].value_counts().nlargest(TOP_N_MODELS).index
        df = df[df['idsmartphone'].isin(top_ids)]

        return df
    except Exception as e:
        print(f"Erreur chargement données Rakuten : {e}")
        return pd.DataFrame(columns=columns)

app = Dash(__name__)
data = load_and_clean_rakuten_data()

app.layout = html.Div([
    html.H1("Rakuten - Suivi des prix des smartphones", style={'textAlign': 'center'}),
    dcc.Graph(id='rakuten-price-trends')
])

@app.callback(
    Output('rakuten-price-trends', 'figure'),
    Input('rakuten-price-trends', 'id')
)
def update_rakuten_graph(_):
    fig = px.line(
        data,
        x="Rounded_Timestamp",
        y="price",
        color="seller",
        line_group="idsmartphone",
        facet_col="offertype",  # Column per offer type
        facet_row="idsmartphone",  # Row per smartphone model
        line_shape="spline",
        title="Tendances de prix sur Rakuten par type d'offre",
        labels={
            "Rounded_Timestamp": "Date",
            "price": "Prix (€)",
            "seller": "Vendeur",
            "idsmartphone": "Modèle",
            "offertype": "Type d'offre"
        },
        height=1600
    )
    return fig

class CSVWatcher(FileSystemEventHandler):
    def on_modified(self, event):
        global data
        if event.src_path == csv_file:
            print(f"Fichier {csv_file} modifié, rechargement des données...")
            data = load_and_clean_rakuten_data()

def start_csv_watcher():
    event_handler = CSVWatcher()
    observer = Observer()
    observer.schedule(event_handler, path=csv_file, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == '__main__':
    threading.Thread(target=start_csv_watcher, daemon=True).start()
    app.run(debug=False, host='157.159.195.72', port=8052)
