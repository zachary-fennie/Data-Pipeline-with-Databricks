"""
EXTRACT
Extract a dataset from a url
"""

import os
import requests


def extract(
    url="https://github.com/fivethirtyeight/data/raw/e6bbbb2d35310b5c63c2995a0d03d582d0c7b2e6/covid-geography/mmsa-icu-beds.csv",
    file_path="covid-geography/mmsa-icu-beds.csv",
):
    """Extract a URL to a file path."""
    print("Extracting data...")

    # Ensure the directory exists
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Raise an error for bad responses

        with open(file_path, "wb") as f:
            f.write(response.content)

        print(f"Data successfully extracted to {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        print(f"Error during requests to {url}: {e}")
        return None
    except IOError as e:
        print(f"Error writing to file {file_path}: {e}")
        return None
