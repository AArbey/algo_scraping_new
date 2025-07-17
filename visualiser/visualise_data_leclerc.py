import logging
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time
import io

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Path to the CSV file
csv_file = '/home/scraping/algo_scraping/LECLERC/product_details.csv'

# Define column names for the CSV
columns = [
    "Platform", "Product", "Seller", "Price", "Delivery Fees", "Delivery Date", "Product State", "Seller Rating", "Timestamp"
]

# Function to load and clean the data
def load_and_clean_data():
    logger.info("Loading and cleaning data from %s", csv_file)
    try:
        with open(csv_file, 'r') as file:
            raw_data = file.read()
    except Exception as e:
        logger.error("Failed to read CSV file: %s", e)
        return pd.DataFrame()  # return empty DataFrame on error

    # Split data into sections by model
    sections = raw_data.split('-' * 132)
    cleaned_data = []

    for idx, section in enumerate(sections):
        lines = section.strip()
        if not lines:  # Ensure the section has data
            continue
        try:
            logger.debug("Processing section %d", idx)
            # Use pandas to read the section as CSV
            model_data = pd.read_csv(
                io.StringIO(lines),
                names=columns,
                skiprows=1,
                engine='python',
                quotechar='"'  # Handle quoted fields properly
            )
            # Skip sections with zero rows to avoid iloc errors
            if model_data.empty:
                continue

            # If the first line of the section is "Non trouvé", we skip the whole section
            if model_data['Product'].iloc[0] == "Non trouvé":
                continue
            
            # On compare chaque ligne avec la première ligne de la section
            # Si les vendeurs sont les mêmes, on garde la première ligne
            if len(model_data) > 1:
                for i in range(1, len(model_data)):
                    if model_data['Seller'].iloc[i] == model_data['Seller'].iloc[0]:
                        model_data = model_data.drop(index=i)
                        break
                # Remove the last line if 'Price' is "Non trouvé"
                if model_data['Price'].iloc[-1] == "Non trouvé":
                    model_data = model_data.iloc[:-1]
                    # Print the new last line's price as well as the first line's price
                    #print(f"Last line price: {model_data['Price'].iloc[-1]}, First line price: {model_data['Price'].iloc[0]}")



            model_data['Timestamp'] = pd.to_datetime(
                model_data['Timestamp'], format='%d/%m/%Y %H:%M:%S', errors='coerce'
            )  # Specify the date format explicitly


            # Si la dernière ligne est vendue par E.Leclerc et que son prix est le même que la première ligne de la section
            # only apply when we have at least two rows
            if len(model_data) > 2 and model_data['Seller'].iloc[-1] == 'E.Leclerc' \
               and model_data['Price'].iloc[-1] == model_data['Price'].iloc[0]:
                # Alors on insère la dernière ligne en haut de la section, et on shift les vendeurs de manière à ce que la deuxième ligne ait maintenant le vendeur de la première, la troisième ligne ait le vendeur de la deuxième etc.. 
                print("Shifting sellers for model:", model_data['Product'].iloc[0])
                model_data = pd.concat([model_data.iloc[-1:], model_data.iloc[:-1]], ignore_index=True)
                model_data['Seller'] = model_data['Seller'].shift(1)

                # Puis on supprime la première ligne
                model_data = model_data.iloc[1:]

            model_data = model_data[model_data['Timestamp'].notnull()]  # Remove invalid timestamps
            if not model_data.empty:
                first_timestamp = model_data['Timestamp'].iloc[0]
                model_data['Timestamp'] = first_timestamp  # Replace all timestamps with the first one
                cleaned_data.append(model_data)
        except Exception as e:
            logger.error("Error processing section %d: %s", idx, e)

    try:
        data = pd.concat(cleaned_data, ignore_index=True)

        # Further cleaning
        data['Price'] = data['Price'].str.replace('€', '').str.replace(',', '.').str.strip()
        data = data[data['Price'].notnull()]
        data = data[data['Price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
        data['Price'] = data['Price'].astype(float)
        data = data[data['Price'] <= 2000]  # Filter out rows with Price > 2000€
        data['Seller Rating'] = pd.to_numeric(data['Seller Rating'], errors='coerce')  # Convert Seller Rating to float

        logger.info("Data loaded: %d rows", len(data))
        return data
    except ValueError:
        logger.warning("No data to concatenate, returning empty DataFrame")
        return pd.DataFrame()

# Initialize the Dash app
app = Dash(__name__)

# Initial data load
data = load_and_clean_data()

# Function to create the figure
def create_figure(data):
    return px.line(
        data,
        x="Timestamp",
        y="Price",
        color="Seller",
        line_group="Product",
        facet_col="Product",
        facet_col_wrap=3,
        line_shape="spline",
        title="Tendances de prix sur Leclerc",
        labels={
            "Timestamp": "Date",
            "Price": "Price (€)",
            "Seller": "Seller",
            "Product": "Product Name"
        },
        height=800
    )

# Define the layout of the app
app.layout = html.Div([
    html.H1("Leclerc - Suivi des prix des smartphones", style={'textAlign': 'center'}),
    dcc.Graph(id='price-trends-graph')  # Dynamic graph
])

# Callback to update the graph when data changes
@app.callback(
    Output('price-trends-graph', 'figure'),
    Input('price-trends-graph', 'id')  # Dummy input to trigger updates
)
def update_graph(_):
    global data
    return create_figure(data)

# File watcher to reload data when the CSV file changes
class CSVFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path == csv_file:
            logger.info("File modified: %s, reloading data", csv_file)
            global data
            data = load_and_clean_data()

# Start the file watcher in a separate thread
def start_file_watcher():
    logger.info("Starting file watcher thread")
    event_handler = CSVFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=csv_file, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher")
        observer.stop()
    observer.join()

# Run the app and file watcher
if __name__ == '__main__':
    logger.info("Launching CSV watcher and Dash app")
    threading.Thread(target=start_file_watcher, daemon=True).start()
    logger.info("Starting Dash on host=0.0.0.0 port=8050")
    app.run(debug=False, host='0.0.0.0', port=8050, use_reloader=False)
