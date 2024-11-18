"""
TRANSFORM AND LOAD
Transforms and Loads data into Databricks
"""

import csv
import os
from databricks import sql
from dotenv import load_dotenv


# load the csv file and insert into Databricks
def load(dataset="covid-geography/mmsa-icu-beds.csv"):
    """Transforms and Loads data into Databricks"""
    print("Uploading data...")
    payload = csv.reader(open(dataset, newline="", encoding="utf-8"), delimiter=",")
    next(payload)
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS icu (MMSA STRING, total_percent_at_risk STRING, 
                high_risk_per_ICU_bed FLOAT, high_risk_per_hospital FLOAT, 
                icu_beds INT, hospitals INT, total_at_risk FLOAT);"""
            )

            cursor.execute("SELECT * FROM icu")
            result = cursor.fetchall()
            if not result:
                # NA values caused typecasting errors which require the following code block
                string_sql = "INSERT INTO icu VALUES"
                values_list = []

                for i in payload:
                    # prepare a list for the cleaned values
                    clean_values = []
                    for value in i:
                        if value == "NA":
                            # append with NULL
                            clean_values.append("NULL")
                        elif isinstance(value, str):
                            # enclose strings in single quotes
                            clean_values.append(f"'{value}'")
                        else:
                            # keep all other values unchanged
                            clean_values.append(value)
                    # create formatted string for the tuple
                    values_list.append(f"({', '.join(map(str, clean_values))})")
                # join the tuples and finalize the SQL statement
                string_sql += "\n" + ",\n".join(values_list) + ";"

                cursor.execute(string_sql)

            cursor.close()
            connection.close()
    print("Upload complete")
    return "Upload Complete"


if __name__ == "__main__":
    load()
