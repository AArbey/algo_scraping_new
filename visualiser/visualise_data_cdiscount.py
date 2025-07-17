import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time
import os
import re

# Path to the Cdiscount CSV file
csv_file = '/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv'

# Columns expected in Cdiscount CSV
columns = [
    "Platform", "Product Name", "Price", "Seller", "Seller Status",
    "Seller Rating", "Delivery Fee", "Timestamp"
]

def clean_price(price):
    if pd.isnull(price):
        return None

    # Convertir en chaîne de caractères et enlever les espaces inutiles
    price_str = str(price).strip()

    # Si le prix est sous la forme "xxx,xx €"
    match = re.match(r'^(\d+),(\d{2})\s?€$', price_str)
    if match:
        euros, cents = match.groups()
        return float(f"{euros}.{cents}")

    # Si le prix est sous la forme "xxx€xx"
    match = re.match(r'^(\d+)[€e](\d{2})$', price_str)
    if match:
        euros, cents = match.groups()
        return float(f"{euros}.{cents}")

    return None

def wrap_labels(text, width=30):
    return '<br>'.join(re.findall('.{1,' + str(width) + '}(?:\\s+|$)', text))

# Function to load and clean the data
def load_and_clean_data():
    # Lire le CSV sans noms de colonnes
    raw_data = pd.read_csv(csv_file, sep=",", header=None, engine='python', on_bad_lines='warn', dtype=str)
    # Supprimer les lignes séparatrices
    raw_data = raw_data[~raw_data[0].astype(str).str.startswith("-")].copy()
    # Deux formats possibles : 5 colonnes (sans Seller Status, Seller Rating, Delivery Fee) ou 8 colonnes
    data_rows = []
    for _, row in raw_data.iterrows():
        row = row.dropna().tolist()
        if len(row) == 5:
            # Format court : Platform, Product Name, Price, Seller, Timestamp
            d = {
                "Platform": row[0],
                "Product Name": row[1],
                "Price": row[2],
                "Seller": row[3],
                "Seller Status": "N/A",
                "Seller Rating": "N/A",
                "Delivery Fee": "N/A",
                "Timestamp": row[4]
            }
            data_rows.append(d)
        elif len(row) >= 8:
            # Format long : Platform, Product Name, Price, Seller, Seller Status, Seller Rating, Delivery Fee, Timestamp
            d = {
                "Platform": row[0],
                "Product Name": row[1],
                "Price": row[2],
                "Seller": row[3],
                "Seller Status": row[4],
                "Seller Rating": row[5],
                "Delivery Fee": row[6],
                "Timestamp": row[7]
            }
            data_rows.append(d)
        # Sinon, ignorer la ligne

    data = pd.DataFrame(data_rows, columns=columns)

    # Clean and convert 'Price'
    data['Price'] = data['Price'].apply(clean_price)
    data = data[data['Price'].notnull()]

    # Clean and convert 'Timestamp'
    data['Timestamp'] = pd.to_datetime(data['Timestamp'], errors='coerce', dayfirst=True)
    data = data[data['Timestamp'].notnull()]
    data['Rounded_Timestamp'] = data['Timestamp'].dt.round('5min')

    data['Product Name'] = data['Product Name'].apply(lambda x: wrap_labels(str(x), width=30))
    # Fill missing values with "N/A" for specific columns
    data = data.fillna("N/A")

    return data

# Initialize the Dash app
app = Dash(__name__)

# Initial data load
data = load_and_clean_data()

# Create the figure
def create_figure(data):
    return px.line(
        data,
        x="Rounded_Timestamp",
        y="Price",
        color="Seller",
        line_group="Product Name",
        facet_col="Product Name",
        facet_col_wrap=3,
        line_shape="spline",
        title="Tendance de prix sur Cdiscount",
        labels={"Rounded_Timestamp": "Date", "Price": "Prix (€)", "Seller": "Vendeur"},
        height=1500
    )

# App layout
app.layout = html.Div([
    html.H1("Cdiscount - Suivi des prix des iPhones", style={'textAlign': 'center'}),
    dcc.Graph(id='price-trends-graph')
])

# Callback for dynamic update
@app.callback(
    Output('price-trends-graph', 'figure'),
    Input('price-trends-graph', 'id')  # Dummy input to trigger update
)
def update_graph(_):
    global data
    return create_figure(data)

# File watcher
class CSVFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global data
        if os.path.abspath(event.src_path) == os.path.abspath(csv_file):
            print(f"Le fichier {csv_file} a changé. Rechargement des données...")
            data = load_and_clean_data()

def start_file_watcher():
    event_handler = CSVFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(csv_file), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# Run app + file watcher
if __name__ == '__main__':
    threading.Thread(target=start_file_watcher, daemon=True).start()
    app.run(debug=True, host='157.159.195.72', port=8054)
