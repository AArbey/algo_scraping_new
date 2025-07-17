import pandas as pd

def main():
    df = pd.read_csv("/home/scraping/algo_scraping/LECLERC/product_details.csv")

    # Remove any rows that are just the long separator line
    sep = "------------------------------------------------------------------------------------------------------------------------------------"
    df = df[~df.apply(lambda row: row.astype(str).eq(sep).any(), axis=1)]


    # 2) Prepare for batch‐ID assignment
    seen = set()
    batch_id = 0
    batch_ids = []

    # 3) Iterate rows and assign batch IDs
    for name, seller, state in zip(df["Product Name"], df["Seller"], df["Product State"]):
        key = (name, seller, state)
        if key in seen:
            # same product from the same seller reappeared → start a new batch
            batch_id += 1
            seen.clear()
        seen.add(key)
        batch_ids.append(batch_id)

    # 4) Add new column and save
    df["batch_id"] = batch_ids
    # 5) Save fully cleaned output
    output_path = "/home/scraping/algo_scraping/LECLERC/product_details.csv"
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()
