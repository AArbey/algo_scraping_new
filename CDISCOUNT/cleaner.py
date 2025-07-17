# This program takes in a .csv (scraping_cdiscount.csv), 
# and removes every line where Product name doesn't start with APPLE, iPhone, or N/A or "Product Name"

# It then saves the cleaned data to a new .csv file (scraping_cdiscount_clean.csv).

import pandas as pd
import os
from pathlib import Path
import re
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import logging
import time

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Path to the Cdiscount CSV file
csv_file = Path(os.getenv("CDISCOUNT_CSV_FILE", "/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv"))
# Path to the cleaned CSV file
cleaned_csv_file = Path(os.getenv("CDISCOUNT_CLEANED_CSV_FILE", "/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount_clean.csv"))
# Columns expected in Cdiscount CSV
columns = [
    "Platform", "Product Name", "Price", "Seller", "Seller Status",
    "Seller Rating", "Delivery Fee", "Timestamp"
]       

def clean_price(price: Optional[str]) -> Optional[float]:
    if price is None:
        return None
    # Ensure price is treated as text
    price_str = str(price)
    # Remove currency symbols and commas
    price_str = re.sub(r"[€$]", "", price_str)
    price_str = price_str.replace(",", ".")
    try:
        return float(price_str)
    except ValueError:
        return None
    
def wrap_labels(text: str, width: int = 30) -> str:
    """Wrap text to a specified width."""
    return '<br>'.join(re.findall('.{1,' + str(width) + '}(?:\\s+|$)', text))

def load_and_clean_data() -> pd.DataFrame:
    start = time.perf_counter()
    df = pd.read_csv(csv_file, usecols=columns)
    # 1) Replace NaN in product names to avoid masking errors
    df["Product Name"].fillna("", inplace=True)
    # 2) Filter with na=False so startswith returns only booleans
    df = df[df["Product Name"]
                .str.startswith(("APPLE", "iPhone"), na=False)]
    df["Price"] = df["Price"].apply(clean_price)
    df["Product Name"] = df["Product Name"].apply(wrap_labels)

    # 3) Fix misaligned timestamp: if it's stuck in Seller Status, shift it to Timestamp
    mask = df["Seller Status"].str.match(
        r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}", na=False
    )
    df.loc[mask, "Timestamp"] = df.loc[mask, "Seller Status"]
    df.loc[mask, ["Seller Status", "Seller Rating", "Delivery Fee"]] = ""

    end = time.perf_counter()
    logging.info(f"Data loaded and cleaned in {end - start:.2f}s (rows: {len(df)})")
    return df

def save_cleaned_data(df: pd.DataFrame) -> None:
    start = time.perf_counter()
    """Save the cleaned DataFrame to a CSV file."""
    df.to_csv(cleaned_csv_file, index=False, encoding='utf-8')
    end = time.perf_counter()
    logging.info(f"Saved cleaned data to {cleaned_csv_file} in {end - start:.2f}s")

def main():
    start_main = time.perf_counter()
    """Main function to load, clean, and save the data."""
    if not csv_file.exists():
        print(f"CSV file {csv_file} does not exist.")
        return
    
    df = load_and_clean_data()
    save_cleaned_data(df)
    total = time.perf_counter() - start_main
    logging.info(f"Total processing time: {total:.2f}s")

if __name__ == "__main__":
    main()