import pandas as pd

def load_smartphone_models_from_excel():
    """Load smartphone models from the Excel file."""
    try:
        df_excel = pd.read_excel('../ID_EXCEL.xlsx', skiprows=7, header=None, dtype=str)
        df_excel = df_excel.iloc[:, 1:3]  # Columns B and C
        df_excel.columns = ['Phone', 'idsmartphone']
        return df_excel.dropna()
    except:
        return pd.DataFrame(columns=['Phone', 'idsmartphone'])

def parse_timestamp(timestamp_str, pfid):
    """Parse the timestamp based on the pfid."""
    try:
        if pfid == 'AMAZ':
            return pd.to_datetime(timestamp_str, format='%Y-%m-%d %H:%M:%S')
        elif pfid == 'RAK':
            return pd.to_datetime(timestamp_str, format='%Y/%m/%d %H:%M')
        elif pfid == 'FNAC':
            return pd.to_datetime(timestamp_str, format='%Y%m%d_%H%M%S')
        else:
            return pd.to_datetime(timestamp_str, errors='coerce')
    except:
        return pd.NaT

def clean_parquet_data():
    """Clean and structure the parquet-compiled CSV data."""
    input_csv = "merged_data.csv"
    output_csv = "./data/parquet_data_cleaned.csv"

    try:
        # Specify dtype for columns with mixed types to avoid DtypeWarning
        dtype_mapping = {
            'offerdetails': 'str',  
            'shipcountry': 'str',  
            'sellercountry': 'str'  
        }
        df = pd.read_csv(input_csv, dtype=dtype_mapping, low_memory=False)

        # Clean Price: Handle duplicates (assuming 'Price' is correct, rename 'price' if necessary)
        # Merge 'price' and 'Price' columns into one
        if 'Price' in df.columns and 'price' in df.columns:
            df['Price'] = df['Price'].fillna(df['price'])
            df.drop(columns=['price'], inplace=True)
        elif 'price' in df.columns:
            df.rename(columns={'price': 'Price'}, inplace=True)

        
        smartphone_models = load_smartphone_models_from_excel()

        # Merge smartphone data
        df = df.merge(smartphone_models, on='idsmartphone', how='left')
        df['idsmartphone'] = df['Phone']
        df.drop(columns=['Phone'], inplace=True)

        # Rename 'idsmartphone' to 'Product Name' and remove 'descriptsmartphone'
        df.rename(columns={'idsmartphone': 'Product Name'}, inplace=True)
        df.drop(columns=['descriptsmartphone'], inplace=True)

        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

        # Clean Delivery Fee (shipcost)
        df['shipcost'] = df['shipcost'].fillna(0).astype(float)

        # Parse Timestamp
        df['timestamp'] = df.apply(lambda row: parse_timestamp(row['timestamp'], row['pfid']), axis=1)
        df['Year'] = df['timestamp'].dt.year
        df['Month'] = df['timestamp'].dt.month
        df['Day'] = df['timestamp'].dt.day
        df['Hour'] = df['timestamp'].dt.hour
        df['Minute'] = df['timestamp'].dt.minute
        df['Second'] = df['timestamp'].dt.second
        df.drop(columns=['timestamp'], inplace=True)

        # Map offertype to state
        df['state'] = df['offertype'].replace({
            'NewCondition': 'Neuf',
            'UsedCondition': 'Occasion',
            'Neuf': 'Neuf',
            "new": 'Neuf',
            "D'occasion - Comme neuf": 'Occasion',
            "D'occasion - Très bon": 'Occasion',
            "D'occasion - Bon": 'Occasion',
            "D'occasion - Acceptable": 'Occasion',
            "acceptable": 'Occasion',
            "very-good": 'Occasion',
            "good": 'Occasion',
            "like-new" : 'Occasion',

        })

        print("Filling in missing states from seller (Amazon Seconde main)...")
        df.loc[(df['seller'] == 'Amazon Seconde main') & (df['state'].isnull() | (df['state'] == '')), 'state'] = 'Occasion'

        # Map pfid to Platform
        platform_mapping = {
            'AMAZ': 'Amazon',
            'RAK': 'Rakuten',
            'FNAC': 'Fnac'
        }
        df['Platform'] = df['pfid'].map(platform_mapping)

        # Rename and select columns
        df.rename(columns={
            'seller': 'Seller',
            'shipcost': 'Delivery Fee',
            'rating': 'Seller Rating'
        }, inplace=True)

        # Add static columns
        df['Seller Status'] = "N/A"  # Not available in source data

        # Link phone models like in clean_rakuten_data
        

        # Drop unnecessary columns
        df.drop(columns=[
            'pfid', 'url', 'offerdetails', 'shipcountry', 
            'sellercountry', 'ratingnb', 'offertype'
        ], inplace=True)

        # Remove prices over 3000€ and below 400€

        print("Removing prices over 3000€ and below 400€")
        df = df[df['Price'] <= 3000 & df['Price'] >= 400]

        # Reorder columns to match target structure
        df = df[[
            'Platform', 'Product Name', 'Price', 'Seller', 'Seller Status',
            'Seller Rating', 'Delivery Fee', 'Year', 'Month', 'Day',
            'Hour', 'Minute', 'Second', 'state'
        ]]

        print("Saving cleaned data...")
        # Save cleaned data
        df.to_csv(output_csv, index=False, sep='|', encoding='utf-8')
        print(f"Cleaned data saved to {output_csv}")

    except Exception as e:
        print(f"Error cleaning parquet data: {e}")

if __name__ == "__main__":
    clean_parquet_data()