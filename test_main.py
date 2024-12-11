"""
TESTING FOR MAIN
"""

import library
import os
from databricks import sql
from dotenv import load_dotenv

import library.extract
import library.query_join


def test_extract():
    extract_test = library.extract.extract()
    assert extract_test is not None


# def test_pyspark_process():
#     pyspark_test = library.extract.pyspark_process()


def test_load():
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM icu")
            result = cursor.fetchall()
            cursor.close()
            connection.close()
    assert result is not None


def test_query():
    query_test = library.query_join.complex_query()
    assert query_test == "Join Successful"


def test_function():
    return True


if __name__ == "__main__":
    assert test_function() is True
