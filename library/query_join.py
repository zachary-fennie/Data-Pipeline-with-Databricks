"""
QUERY
Query the database & join a table
"""

import os
from databricks import sql
from dotenv import load_dotenv


SQL_QUERY = """ WITH median_values AS (
    SELECT 
        MMSA,
        MEDIAN(high_risk_per_ICU_bed) AS median_ICU_bed,
        MEDIAN(high_risk_per_hospital) AS median_hospital
    FROM 
        icu
    GROUP BY 
        MMSA
) 

SELECT 
    icu.*,
    mv.median_ICU_bed,
    mv.median_hospital
FROM 
    icu
JOIN 
    median_values mv ON icu.MMSA = mv.MMSA;
"""


def complex_query():
    """Complex query - join in Databricks"""
    print("Querying data...")
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL_QUERY)

            # result = cursor.fetchall()
            # for row in result:
            #     print(row)

            cursor.close()
            connection.close()
    print("Complex Query Successful")
    return "Join Successful"


if __name__ == "__main__":
    complex_query()
