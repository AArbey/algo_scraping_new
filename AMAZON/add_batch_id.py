import pandas as pd

def main():
    df = pd.read_csv("/home/scraping/algo_scraping/AMAZON/amazon_offers.csv")


    # 2) Prepare for batch‐ID assignment
    seen = set()
    batch_id = 0
    batch_ids = []

    # 3) Iterate rows and assign batch IDs
    for url, seller, offertype in zip(df["url"], df["seller"], df["offertype"]):
        # ignore this seller for batch logic, but keep row
        if seller == "Amazon Seconde main":
            batch_ids.append(batch_id)
            continue
        key = (url, seller, offertype)
        if key in seen:
            # same product from the same seller reappeared → start a new batch
            batch_id += 1
            seen.clear()
        seen.add(key)
        batch_ids.append(batch_id)

    # 4) Add new column and save
    df["batch_id"] = batch_ids
    # 5) Save fully cleaned output
    output_path = "/home/scraping/algo_scraping/AMAZON/amazon_offers.csv"
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()
