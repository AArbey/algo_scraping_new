import pandas as pd
import matplotlib.pyplot as plt

# Define the desired column order for consistency
desired_columns = [
    'Platform', 'Product Name', 'Price', 'Seller', 'Seller Status',
    'Seller Rating', 'Delivery Fee', 'state', 'Year', 'Month',
    'Day', 'Hour', 'Minute', 'Second'
]

# Read each CSV file with the correct delimiter
rakuten_df = pd.read_csv('data/Rakuten_data_cleaned.csv', sep='|')
carrefour_df = pd.read_csv('data/scraping_carrefour_cleaned.csv', sep='|')
cdiscount_df = pd.read_csv('data/scraping_cdiscount_cleaned.csv', sep='|')

# Reorder the columns for Rakuten to match the desired order
rakuten_df = rakuten_df[desired_columns]

# Combine all dataframes
combined_df = pd.concat([rakuten_df, carrefour_df, cdiscount_df], ignore_index=True)

# Drop rows where Price > 3500
combined_df = combined_df[combined_df['Price'] <= 3500]

# Drop rows where Price < 400
combined_df = combined_df[combined_df['Price'] >= 400]

# Check for rows where Price is not a number
non_numeric_price_row = combined_df[pd.to_numeric(combined_df['Price'], errors='coerce').isna()]
if not non_numeric_price_row.empty:
    print("First row where Price is not a number:")
    print(non_numeric_price_row.iloc[0])
else:
    print("All rows in the Price column are numeric.")

# Normalize product names to have consistent naming
name_mapping = {
    'Iphone 16 256Go Noir Titane': 'iPhone 16 256GB Noir Titanium',
    'Iphone 16 128Go Noir Titane': 'iPhone 16 128GB Noir Titanium',
    'Iphone 16 plus 512Go Noir': 'iPhone 16 Plus 512GB Noir',
    'Iphone 16 plus 256Go Noir': 'iPhone 16 Plus 256GB Noir',
    'Iphone 16 pro 1T Noir Titanium': 'iPhone 16 Pro 1TB Noir Titanium',
    'Iphone 16 pro max 1T Noir Titane': 'iPhone 16 Pro Max 1TB Noir Titanium',
    'Iphone 15 512Go Noir': 'iPhone 15 512GB Noir',
    'Iphone 15 256Go Noir': 'iPhone 15 256GB Noir',
    'Iphone 15 128Go Noir': 'iPhone 15 128GB Noir',
    'Iphone 15+ 512Go Noir': 'iPhone 15 Plus 512GB Noir',
    'Iphone 15+ 256Go Noir': 'iPhone 15 Plus 256GB Noir',
    'Iphone 15+ 128Go Noir': 'iPhone 15 Plus 128GB Noir',
    'Iphone 15 pro 1T Noir Titanium': 'iPhone 15 Pro 1TB Noir Titanium',
    'Iphone 15 pro max 1T Noir Titane': 'iPhone 15 Pro Max 1TB Noir Titanium',
    'iPhone 16 256 Go Noir (MYEE3ZD/A) APPLE': 'iPhone 16 256GB Noir Titanium',
    'iPhone 16 Plus 512 Go Noir (MY1P3ZD/A) APPLE': 'iPhone 16 Plus 512GB Noir',
    'iPhone 16 Plus 256 Go Noir (MXWN3ZD/A) APPLE': 'iPhone 16 Plus 256GB Noir',
    'iPhone 16 Plus 128 Go Noir (MXVU3ZD/A) APPLE': 'iPhone 16 Plus 128GB Noir',
    'Apple Iphone 16 15,5 Cm (6.1") Double Sim Ios 18 5g Usb Type-c 128 Go Noir': 'iPhone 16 128GB Noir Titanium',
    'iPhone 16 Pro Max 1 To Noir titane (MYX43ZD/A) APPLE': 'iPhone 16 Pro Max 1TB Noir Titanium',
    'iPhone 15 512 Go Noir (MTPC3ZD/A) APPLE': 'iPhone 15 512GB Noir',
    'iPhone 15 256 Go Noir (MTP63ZD/A) APPLE': 'iPhone 15 256GB Noir',
    'iPhone 15 128 Go Noir (MTP03ZD/A) APPLE': 'iPhone 15 128GB Noir',
    'iPhone 15 Plus 512 Go Noir (MU1H3ZD/A) APPLE': 'iPhone 15 Plus 512GB Noir',
    'iPhone 15 Plus 256 Go Noir (MU183ZD/A) APPLE': 'iPhone 15 Plus 256GB Noir',
    'iPhone 15 Plus 128 Go Noir (MU0Y3ZD/A) APPLE': 'iPhone 15 Plus 128GB Noir',
    'iPhone 15 Pro 1 To Titane noir (MTVC3ZD/A) APPLE': 'iPhone 15 Pro 1TB Noir Titanium',
    'APPLE iPhone 16 Pro 256GB Black Titanium': 'iPhone 16 Pro 256GB Noir Titanium',
    'APPLE iPhone 15 Pro Max 256GB Natural Titanium (2023)': 'iPhone 15 Pro Max 256GB Natural Titanium',
    'APPLE iPhone 16 128GB Black': 'iPhone 16 128GB Noir Titanium',
    'APPLE iPhone 16 Plus 512GB Black': 'iPhone 16 Plus 512GB Noir',
    'APPLE iPhone 15 Pro 128GB Blue Titanium (2023)': 'iPhone 15 Pro 128GB Blue Titanium',
    'APPLE iPhone 16 Pro 1TB Black Titanium': 'iPhone 16 Pro 1TB Noir Titanium',
    'APPLE iPhone 15 256GB Pink (2023)': 'iPhone 15 256GB Pink',
    'APPLE iPhone 14 Pro Max 128GB Deep Purple': 'iPhone 14 Pro Max 128GB Deep Purple',
    'APPLE iPhone 15 Plus 128GB Pink (2023)': 'iPhone 15 Plus 128GB Pink',
    'APPLE iPhone 14 Pro Max 128GB Space Black': 'iPhone 14 Pro Max 128GB Space Black',
    'iPhone 15 128GB Noir': 'iPhone 15 128GB Noir',
    'APPLE iPhone 15 128GB Pink (2023)': 'iPhone 15 128GB Pink',
    'iPhone 15 Pro 1TB Noir Titanium': 'iPhone 15 Pro 1TB Noir Titanium',
    'APPLE iPhone 14 Pro Max 128GB Silver': 'iPhone 14 Pro Max 128GB Silver',
    'APPLE iPhone 15 Pro 128GB White Titanium (2023)': 'iPhone 15 Pro 128GB White Titanium',
    'APPLE iPhone 16 512GB Black': 'iPhone 16 512GB Noir Titanium',
    'APPLE iPhone 16 128GB Pink': 'iPhone 16 128GB Pink',
    'APPLE iPhone 16 Pro Max 1TB Black Titanium': 'iPhone 16 Pro Max 1TB Noir Titanium',
    'APPLE iPhone 16 Plus 128GB Black': 'iPhone 16 Plus 128GB Noir',
    'APPLE iPhone 15 Pro 128GB Black Titanium (2023)': 'iPhone 15 Pro 128GB Noir Titanium',
    'APPLE iPhone 15 Pro Max 256GB White Titanium (2023)': 'iPhone 15 Pro Max 256GB White Titanium',
    'APPLE iPhone 16 Pro Max 256GB Desert Titanium': 'iPhone 16 Pro Max 256GB Desert Titanium',
    'APPLE iPhone 16 256GB Black': 'iPhone 16 256GB Noir Titanium',
    'APPLE iPhone 16 Pro 128GB Desert Titanium': 'iPhone 16 Pro 128GB Desert Titanium'
}

combined_df['Product Name'] = combined_df['Product Name'].replace(name_mapping)

# Print each unique value in the Product Name column
unique_product_names = combined_df['Product Name'].unique()
print("Unique Product Names:")
for product_name in unique_product_names:
    print(product_name)

# Scramble the rows by sampling all rows in random order
scrambled_df = combined_df.sample(frac=1).reset_index(drop=True)

# Save the scrambled data to a new CSV file
scrambled_df.to_csv('data/combined_scrambled_data.csv', sep='|', index=False)

# Plot a graph of all the prices
plt.figure(figsize=(10, 6))
plt.plot(scrambled_df.index, scrambled_df['Price'], marker='o', linestyle='-', color='b', label='Price')
plt.title('Price Distribution')
plt.xlabel('Index')
plt.ylabel('Price')
plt.legend()
plt.grid(True)
plt.show()

print("Combined and scrambled CSV created successfully!")