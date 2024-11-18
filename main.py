"""
MAIN
"""

from library.extract import extract
from library.transform_load import load
from library.query_join import complex_query


def main():
    """Main function for the Complex SQL operations"""
    # extract
    extract()

    # transform and load
    load()

    # query
    complex_query()


if __name__ == "__main__":
    main()
