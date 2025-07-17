import csv
import matplotlib.pyplot as plt

def save_cleaned_data(data, output_file):
    """Save the cleaned data to a new CSV file with '|' as the separator."""
    with open(output_file, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter='|')
        writer.writeheader()
        writer.writerows(data)

def analyze_csv(file_path, output_file):
    with open(file_path, mode='r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"')
        data = list(reader)

    columns = data[0].keys()
    unique_values = {col: set() for col in columns if col != 'state'}
    prices = []

    data = [
        row for row in data 
        if row.get('Price') not in [None, 'None', 'Price', 'N/A'] and 
           row.get('Product Name') not in ['N/A', 'Console Nintendo Switch 2 • Bleu Clair & Rouge Clair + Mario Kart World (Code)']
    ]  # Remove invalid rows
    for i, row in enumerate(data):
        # Check if the last three columns are empty
        row_values = list(row.values())
        if len(row_values) >= 4 and all(not row_values[-j] for j in range(1, 4)):
            # Insert 'N/A' three times before the last column
            row_values = row_values[:-4] + ['N/A', 'N/A', 'N/A'] + row_values[-4:]
            row = dict(zip(row.keys(), row_values))
            data[i] = row  # Update the modified row in the data list

        for col in row.keys():  # Dynamically handle all columns in the row
            if col not in unique_values:
                unique_values[col] = set()  # Add new columns to unique_values
            if col != 'state':  # Skip 'state' column
                unique_values[col].add(row[col])
        
        # Handle the "Delivery Info" column
        delivery_info = row.get('Delivery Info', '')
        # Si la valeur commence par "Livraison offerte", on remplace par 0
        if delivery_info.startswith("Livraison offerte"):
            row['Delivery Info'] = 0
        
        # Si la valeur commence par "Livraison" puis contient "€", on extrait le montant
        # par exemple "Livraison 4.99€ sous 3 à 6 jours ouvrés"
        elif delivery_info.startswith("Livraison") and "€" in delivery_info:
            try:
                amount = float(delivery_info.split("€")[0].split()[-1].replace(',', '.'))
                row['Delivery Info'] = amount
            except (ValueError, IndexError):
                row['Delivery Info'] = 'N/A'  # Si l'extraction échoue, on remplace par N/A

        # Sinon, on remplace par N/A
        else:
            row['Delivery Info'] = 'N/A'   

        # Handle the "Seller Rating" column
        seller_rating = row.get('Seller Rating', '')

        if seller_rating == 'Non spécifié':
            row['Seller Rating'] = 'N/A'

        # Handle the "state" column
        product_name = row.get('Product Name', '')
        if product_name.endswith('- Reconditionné - Excellent état'):
            row['Product Name'] = product_name.replace('- Reconditionné - Excellent état', '').strip()
            row['state'] = "Reconditionne" 
        else:
            row['state'] = "Neuf"

        timestamp = row.get('Timestamp', '')
        if timestamp:
            try:
                date_part, time_part = timestamp.split(' ')
                day, month, year = date_part.split('/')
                hour, minute, second = time_part.split(':')
                row['Year'] = year
                row['Month'] = month
                row['Day'] = day
                row['Hour'] = hour
                row['Minute'] = minute
                row['Second'] = second
            except ValueError:
                # Handle invalid Timestamp format
                row['Year'] = 'N/A'
                row['Month'] = 'N/A'
                row['Day'] = 'N/A'
                row['Hour'] = 'N/A'
                row['Minute'] = 'N/A'
                row['Second'] = 'N/A'
        else:
            row['Year'] = 'N/A'
            row['Month'] = 'N/A'
            row['Day'] = 'N/A'
            row['Hour'] = 'N/A'
            row['Minute'] = 'N/A'
            row['Second'] = 'N/A'
        # Remove the original Timestamp column
        if 'Timestamp' in row:
            del row['Timestamp']
        
        row_price = row.get('Price')
        if not row_price:
            continue
        # Normalize prices
        price_str = row_price.replace('€', '').replace(',', '.').replace(' ', '')
        if price_str.isdigit() and len(price_str) > 2:
            price_str = f"{price_str[:-2]}.{price_str[-2:]}"  # Handle formats like "82900"
        elif price_str.endswith('00') and '.' not in price_str:
            price_str = price_str[:-2]  # Remove trailing '00' if no decimal point exists
        try:
            row['Price'] = float(price_str)  # Update the row with the normalized price
            prices.append(float(price_str))
        except ValueError:
            continue

    for row in data:

        row["Delivery Fee"] = row.pop("Delivery Info", "N/A")
        row["Seller Status"] = "N/A"

    with open(output_file, mode='w', encoding='utf-8', newline='') as f:
        fieldnames = [
            "Platform",
            "Product Name",
            "Price",
            "Seller",
            "Seller Status",
            "Seller Rating",
            "Delivery Fee",
            "state",
            "Year",  # Add missing fields
            "Month",
            "Day",
            "Hour",
            "Minute",
            "Second"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')
        writer.writeheader()
        writer.writerows(data)

    # Display unique values in the second column (Product Name)
    #product_names = unique_values.get('Product Name', set())
    #print(f"Unique Product Names in {file_path}:")
    #for product_name in product_names:
    #    print(product_name)
#
    #plt.hist(prices, bins=10)
    #plt.title('Price Range')
    #plt.xlabel('Price')
    #plt.ylabel('Frequency')
    #plt.show()

if __name__ == "__main__":
    analyze_csv(
        "./data/scraping_carrefour.csv",
        "./data/scraping_carrefour_cleaned.csv"
    )
