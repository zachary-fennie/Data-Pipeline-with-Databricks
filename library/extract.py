"""
EXTRACT
Extract a dataset from a url
"""

import os
import requests
from requests.exceptions import RequestException
from pathlib import Path
from pyspark.sql import SparkSession


def extract(
    url="https://github.com/fivethirtyeight/data/raw/e6bbbb2d35310b5c63c2995a0d03d582d0c7b2e6/covid-geography/mmsa-icu-beds.csv",
    file_path="covid-geography/mmsa-icu-beds.csv",
    retries=3,  # max retries in case of failure
    timeout=10,  # timeout
):
    """Extracts data from a URL and saves it to a file path."""

    print(f"Starting extraction from {url}...")

    # check directory exists
    directory = Path(file_path).parent
    directory.mkdir(parents=True, exist_ok=True)

    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # error for bad responses

            # write into file
            with open(file_path, "wb") as f:
                f.write(response.content)

            print(f"Data successfully extracted to {file_path}")
            return file_path

        except RequestException as e:
            print(f"Error during request to {url}: {e}")
            attempt += 1
            if attempt < retries:
                print(f"Retrying... ({attempt}/{retries})")
            else:
                print(f"Failed after {retries} attempts.")
                return None

        except IOError as e:
            print(f"Error writing to file {file_path}: {e}")
            return None


def pyspark_process(file_path="covid-geography/mmsa-icu-beds.csv"):
    """Process the extracted data using PySpark."""

    # initialize spark session
    spark = SparkSession.builder.appName("DataExtractionAndProcessing").getOrCreate()

    # read data into a spark df
    print(f"Reading data from {file_path}...")
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # filter rows where beds are greater than 100
    df_filtered = df.filter(
        df["icu_beds"] > 100
    )  # Modify according to your column name

    # display filtered data
    df_filtered.show()

    # save data to a new csv
    output_path = "processed_data.csv"
    df_filtered.write.csv(output_path, header=True)

    print(f"Processed data saved to {output_path}")

    spark.stop()
