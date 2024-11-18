[![CI/CD](https://github.com/zachary-fennie/Data-Pipeline-with-Databricks/actions/workflows/CI_CD.yml/badge.svg)](https://github.com/zachary-fennie/Data-Pipeline-with-Databricks/actions/workflows/CI_CD.yml)



# Fennie's Data Pipeline for Databricks
## Building off of previous ETL-CRUD and SQL pipelines, the Data Pipeline for Databricks will extract data from an external source, connect and load the data into a Databricks enivronment, and join the results of a math operation to the table in Databricks with a SQL script.



![diagram-export-10-15-2024-10_40_57-PM](https://github.com/user-attachments/assets/c176c501-b37e-4bc4-88b2-f047fdc31f62)

## Structure
The `library` directory contains `extract.py` to extract raw data from an online url source, `transform_load.py` to transform and load the original raw data from a `.csv` to a Databricks database, and `complex_query.py` to perform the math and join operations through a SQL script.

## Successful SQL Operations
<img width="1071" alt="Screenshot 2024-10-20 at 8 30 17 PM" src="https://github.com/user-attachments/assets/165c22af-3ddb-4b20-b66e-7adcd54a13a3">

### Core Files of the Repo:
* Jupyter notebook
* `library.py`
    - `extract.py`
    - `transform_load.py`
    - `complex_query.py`
* `test_main.py`
* `requirements.txt`
* CI/CD pipeline
* `Makefile`
* `README.md`

## Data
### FiveThirtyEight's MMS ICU Beds Dataset
This dataset combines data from the Centers for Disease Control and Prevention's Behavioral Risk Factor Surveillance System (BRFSS) and the Kaiser Family Foundation to illustrate the number of people who were at high risk for hospitalization from the novel coronavirus COVID-19 in 2020.\
URL: https://github.com/fivethirtyeight/data/blob/e6bbbb2d35310b5c63c2995a0d03d582d0c7b2e6/covid-geography/mmsa-icu-beds.csv

### Summary Statistics of the ICU Dataset
<img width="1056" alt="Screenshot 2024-10-05 at 6 34 57 PM" src="https://github.com/user-attachments/assets/536234ae-e5ff-47dd-b371-b420a96807c0">

### Data Visualization of High Risk Persons per ICU beds & Hopitals
![output](https://github.com/user-attachments/assets/18565095-13cf-46be-b59b-174f677e9536)

### Jupyter Notebook for Preliminary Examination of Data
[Notebook](https://github.com/zfennie/Python-Scripting-for-SQL-Database/blob/989acb7d003177e0bd28f9d42cad90ff8a8fb269/main_notebook.ipynb)
