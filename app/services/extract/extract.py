from app.services.extract import api, parser, storage

from app import config

PAYLOAD_TEMPLATE = config.PAYLOAD_TEMPLATE


def main():
    print("Starting data extraction process...")

    print("Requesting initial page to determine total pages...")
    
    initial_data = api.fetch_page(PAYLOAD_TEMPLATE)
    total_pages = parser.get_total_pages(initial_data)
    
    print(f"Total pages found: {total_pages}")

    all_results = api.fetch_all_pages(PAYLOAD_TEMPLATE, total_pages)

    df = parser.consolidate_results(all_results)
    if df.empty:
        print("No data retrieved. Exiting.")
        exit(0)

    df = storage.add_date_partition_column(df)

    print("Combined DataFrame shape:", df.shape)
    print(df.head())

    storage.save_as_parquet(df)
    
    print("Processing complete.")

if __name__ == "__main__":
    main()
