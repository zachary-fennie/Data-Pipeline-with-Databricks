"""
TRANSFORM AND LOAD
Transforms and Loads data into Databricks
"""

import csv
import os
from databricks import sql
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def load(dataset="covid-geography/mmsa-icu-beds.csv"):
    """Transforms and Loads data into Databricks"""
    print("Uploading data...")

    # read data
    payload = csv.reader(open(dataset, newline="", encoding="utf-8"), delimiter=",")
    next(payload)  # skip header

    load_dotenv()

    # connect to databricks
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:
            # create the table if it doesn't exist
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS icu (MMSA STRING, total_percent_at_risk STRING, 
                high_risk_per_ICU_bed FLOAT, high_risk_per_hospital FLOAT, 
                icu_beds INT, hospitals INT, total_at_risk FLOAT);"""
            )

            cursor.execute("SELECT * FROM icu")
            result = cursor.fetchall()

            if not result:
                # prepare to load data if the table is empty
                string_sql = "INSERT INTO icu VALUES"
                values_list = []

                for i in payload:
                    # clean the data
                    clean_values = []
                    for value in i:
                        if value == "NA":
                            clean_values.append("NULL")
                        elif isinstance(value, str):
                            clean_values.append(f"'{value}'")
                        else:
                            clean_values.append(value)
                    values_list.append(f"({', '.join(map(str, clean_values))})")

                # join values and insert into the table
                string_sql += "\n" + ",\n".join(values_list) + ";"
                cursor.execute(string_sql)

            cursor.close()
            connection.close()

    # load data into pyspark for further transformations
    print("Upload complete")

    # initialize Spark session
    spark = SparkSession.builder.appName("Databricks Data Processing").getOrCreate()

    # load the data from databricks
    df = spark.read.format("delta").table("icu")

    # sample of the data
    # df.show()

    # filtering or aggregating example
    # df_filtered = df.filter(df["icu_beds"] > 100)
    # df_filtered.show()

    # save the processed data to a new delta
    # df_filtered.write.format("delta").save("path_to_save_filtered_data")
    # or csv
    # df_filtered.write.csv("path_to_save_csv", header=True)

    # Stop the spark session
    spark.stop()

    return "Upload Complete"


# if __name__ == "__main__":
#     load()
