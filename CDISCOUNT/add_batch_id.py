import pandas as pd
from cleaner import load_and_clean_data

def main():
    # 1) Load & clean the raw CSV
    df = load_and_clean_data()

    # 2) Prepare for batch‐ID assignment
    seen = set()
    batch_id = 0
    batch_ids = []

    # 3) Iterate rows and assign batch IDs
    for name, seller in zip(df["Product Name"], df["Seller"]):
        key = (name, seller)
        if key in seen:
            # same product from the same seller reappeared → start a new batch
            batch_id += 1
            seen.clear()
        seen.add(key)
        batch_ids.append(batch_id)

    # 4) Add new column and save
    df["batch_id"] = batch_ids
    # 5) Save fully cleaned output
    output_path = "/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv"
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()
