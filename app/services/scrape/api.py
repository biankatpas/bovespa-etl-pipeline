import requests
import json
import base64

from app import config

BASE_URL = config.BASE_URL


def encode_payload(payload):
    """Encodes the payload dictionary to a base64 string."""
    payload_json = json.dumps(payload)
    return base64.b64encode(payload_json.encode("utf-8")).decode("utf-8")

def fetch_page(payload_template):
    """
    Given a payload_template dictionary with updated 'pageNumber',
    encode it and fetch data from the endpoint.
    Returns the JSON response.
    """
    encoded_payload = encode_payload(payload_template)
    url = BASE_URL + encoded_payload
   
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Request for page {payload_template.get('pageNumber')} failed with status code {response.status_code}")
    return response.json()

def fetch_all_pages(payload_template, total_pages):
    """
    Fetch all pages of results from the API.

    Args:
        payload_template (dict): Base payload parameters (with "pageNumber" updated per page).
        total_pages (int): Total number of pages to fetch.

    Returns:
        list: Consolidated list of records from all pages.
    """
    all_results = []
    for page in range(1, total_pages + 1):
        payload_template["pageNumber"] = page
        try:
            page_data = fetch_page(payload_template)
        except Exception as e:
            print(e)
            continue
        results = page_data.get("results", [])
        all_results.extend(results)
    return all_results
