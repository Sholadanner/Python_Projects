# Data Ingestion and Cleaning Project

This project demonstrates a complete data ingestion and cleaning workflow using Python. It reads sales, product, and promotion datasets from various sources (local CSV, Excel, and Google Sheets), performs basic data cleaning, and exports the cleaned data to a Google Cloud Storage (GCS) bucket in Parquet format.

To avoid exposing local file paths or sensitive credentials, the script uses environment variables.

---

## Data Sources

- **Sales data:** Local CSV file  
- **Product data:** Local Excel file  
- **Promotions data:** Public Google Sheets file  

---

##  Features

- Reads datasets from different formats and sources
- Cleans column names (`lowercase`, `strip spaces`, `replace spaces with underscores`)
- Fills missing numeric values with column means
- Removes duplicate rows
- Exports cleaned datasets to Google Cloud Storage in Parquet format

---

## Requirements

- Python 3.8+
- pandas
- gcsfs (for exporting to GCS)



```bash
pip install pandas gcsfs
