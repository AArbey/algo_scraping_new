import pandas as pd

# File paths
csv_file = './data/Rakuten_data.csv'
excel_file = '../ID_EXCEL.xlsx'
output_file = './data/Rakuten_data_cleaned.csv'

def load_smartphone_models_from_excel():
    """Load smartphone models from the Excel file."""
    try:
        df_excel = pd.read_excel(excel_file, skiprows=7, header=None, dtype=str)
        if df_excel.shape[1] >= 3:
            df_excel = df_excel.iloc[:, 1:3]  # Columns B and C
            df_excel.columns = ['Phone', 'idsmartphone']
            return df_excel.dropna()
        else:
            print("Le fichier Excel ne contient pas suffisamment de colonnes.")
            return pd.DataFrame(columns=['Phone', 'idsmartphone'])
    except Exception as e:
        print(f"Erreur lecture Excel : {e}")
        return pd.DataFrame(columns=['Phone', 'idsmartphone'])

def clean_rakuten_data():
    """Clean Rakuten data and link phone models."""
    try:
        df = pd.read_csv(csv_file, sep=",", engine='python', on_bad_lines='skip')
        if df.empty:
            print("Fichier CSV vide.")
            return

        # Clean and normalize data
        df['price'] = df['price'].astype(str).str.replace('€', '').str.replace(',', '.').str.strip()
        df = df[df['price'].notnull()]
        df = df[df['price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
        df['price'] = df['price'].astype(float)

        df['shipcost'] = df['shipcost'].fillna(0).astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y/%m/%d %H:%M', errors='coerce')
        df = df[df['timestamp'].notnull()]
        df['seller'] = df['seller'].fillna("Unknown")

        # Remove unnecessary columns
        df.drop(columns=['pfid','url', 'offerdetails', 'shipcountry', 'sellercountry', 'rating', 'ratingnb'], inplace=True)
        
        # Link phone models
        smartphone_models = load_smartphone_models_from_excel()
        if smartphone_models.empty:
            print("Aucun modèle de smartphone valide trouvé.")
        else:
            df = df.merge(smartphone_models, on='idsmartphone', how='left')
            df['idsmartphone'] = df['Phone']
            df.drop(columns=['Phone'], inplace=True)

        # Add Platform column
        df['Platform'] = "Rakuten"

        # Rename columns
        df.rename(columns={
            'idsmartphone': 'Product Name',
            'price': 'Price',
            'seller': 'Seller',
            'shipcost': 'Delivery Fee',
            'timestamp': 'Timestamp',
            'offertype': 'state'
        }, inplace=True)

        # Add Seller Status and Seller Rating columns
        df['Seller Status'] = "N/A"
        df['Seller Rating'] = "N/A"

        # Map state values
        df['state'] = df['state'].replace({
            'NewCondition': 'Neuf',
            'UsedCondition': 'Occasion'
        })

        # Extract year, month, day, hour, minute, and second from Timestamp
        df['Year'] = df['Timestamp'].dt.year
        df['Month'] = df['Timestamp'].dt.month
        df['Day'] = df['Timestamp'].dt.day
        df['Hour'] = df['Timestamp'].dt.hour
        df['Minute'] = df['Timestamp'].dt.minute
        df['Second'] = df['Timestamp'].dt.second

        # Remove the original Timestamp column
        df.drop(columns=['Timestamp'], inplace=True)

        # Reorder columns
        df = df[['Platform', 'Product Name', 'Price', 'Seller', 'Seller Status', 
                 'Seller Rating', 'Delivery Fee', 'Year', 'Month', 'Day', 
                 'Hour', 'Minute', 'Second', 'state']]

        # Save cleaned data
        df.to_csv(output_file, index=False, sep='|', encoding='utf-8')
        print(f"Données nettoyées sauvegardées dans {output_file}")
    except Exception as e:
        print(f"Erreur nettoyage données Rakuten : {e}")

if __name__ == "__main__":
    clean_rakuten_data()
