import pandas as pd


def get_total_pages(initial_data):
    """
    Extracts the total number of pages from the JSON response.
    Uses the absolute value in case it's negative.
    """
    return abs(initial_data.get("page", {}).get("totalPages", 1))

def consolidate_results(all_results):
    """
    Converts the list of result dictionaries into a Pandas DataFrame.
    """
    df = pd.DataFrame(all_results)
    return df
