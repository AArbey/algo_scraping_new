# Dash visualisation pour les offres Amazon

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

# Chemin vers le fichier CSV Amazon
csv_file = '/home/scraping/algo_scraping/AMAZON/amazon_offers.csv'

# Colonnes attendues dans le fichier CSV
columns = [
    "pfid", "idsmartphone", "url", "timestamp", "Price", "shipcost",
    "rating", "ratingnb", "offertype", "offerdetails",
    "shipcountry", "sellercountry", "seller", "descriptsmartphone"
]

# Fonction de chargement et de nettoyage des données Amazon
def load_and_clean_amazon_data(max_retries=5, delay=1):
    for attempt in range(max_retries):
        try:
            df = pd.read_csv(csv_file, sep=",", engine='python', on_bad_lines='skip')
            if df.empty or df.columns.tolist()[0] != 'pfid':
                raise ValueError("Fichier CSV vide ou mal formé (en cours d'écriture ?)")
            
            df['Price'] = df['Price'].astype(str).str.replace('€', '').str.replace(',', '.').str.strip()
            df = df[df['Price'].notnull()]
            df = df[df['Price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
            df['Price'] = df['Price'].astype(float)
            df = df[df['Price'] > 0]

            df['shipcost'] = pd.to_numeric(df['shipcost'], errors='coerce').fillna(0.0)
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df[df['timestamp'].notnull()]
            df['Rounded_Timestamp'] = df['timestamp'].dt.round('1min')
            df['seller'] = df['seller'].fillna("Unknown")
            df['idsmartphone'] = df['descriptsmartphone']
            df['offertype'] = df['offertype'].fillna("N/A")

            return df

        except Exception as e:
            print(f"Tentative {attempt+1}/{max_retries} – Erreur lecture CSV : {e}")
            time.sleep(delay)

    print("⛔️ Échec de chargement des données après plusieurs tentatives.")
    return pd.DataFrame(columns=columns)


# Initialisation de l'application Dash
app = Dash(__name__)

# Chargement initial des données
data = load_and_clean_amazon_data()

# Fonction de création de la figure
def create_figure(data):
    return px.line(
        data,
        x="Rounded_Timestamp",
        y="Price",
        color="seller",
        line_group="idsmartphone",
        facet_col="idsmartphone",
        facet_col_wrap=3,
        line_shape="spline",
        title="Tendances de prix sur Amazon",
        labels={
            "Rounded_Timestamp": "Date",
            "Price": "Prix (€)",
            "seller": "Vendeur",
            "idsmartphone": "Modèle",
            "offertype": "Type d'offre"
        },
        height=1200
    )

# Layout de l'application
app.layout = html.Div([
    html.H1("Amazon - Suivi des prix des smartphones", style={'textAlign': 'center'}),
    dcc.Graph(id='amazon-price-trends')
])

# Callback pour mettre à jour le graphique
@app.callback(
    Output('amazon-price-trends', 'figure'),
    Input('amazon-price-trends', 'id')
)
def update_graph(_):
    global data
    return create_figure(data)

# Watcher de fichier pour recharger les données automatiquement
class CSVWatcher(FileSystemEventHandler):
    def on_modified(self, event):
        global data
        if event.src_path == csv_file:
            print(f"Fichier {csv_file} modifié, rechargement des données...")
            data = load_and_clean_amazon_data()

# Lancer le watcher dans un thread séparé
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

# Démarrage de l'application
if __name__ == '__main__':
    threading.Thread(target=start_csv_watcher, daemon=True).start()
    app.run(debug=True, host='157.159.195.72', port=8053)
