"""
MAIN
"""

import library.extract
from library.transform_load import load
from library.query_join import complex_query


def main():
    """Main function"""

    library.extract.extract()

    # library.extract.process_with_pyspark()

    load()

    complex_query()


def test_function():
    return True


if __name__ == "__main__":
    test_function()


# if __name__ == "__main__":
# extracted_file_path = extract(
#     url="https://example.com/data.csv", file_path="data/output.csv"
# )

# if extracted_file_path:
#     process_with_pyspark(extracted_file_path)
