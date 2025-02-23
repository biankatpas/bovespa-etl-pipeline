import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from extract import config

OUTPUT_DIR = config.OUTPUT_DIR


def add_date_partition_column(df):
    """Adds a 'date' column with today's date to the DataFrame."""
    today = datetime.today().strftime("%Y-%m-%d")
    df["date"] = today
    return df

def save_as_parquet(df, output_dir=OUTPUT_DIR, partition_col="date"):
    """
    Saves the DataFrame as a single Parquet file using the partition column value in the filename.
    Each run will produce a new file for the given day.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    partition_value = df[partition_col].iloc[0]
    
    filename = f"part-{partition_value}.parquet"
    filepath = os.path.join(output_dir, filename)
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath)
    