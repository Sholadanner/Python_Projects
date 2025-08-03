# Importing libraries
import pandas as pd
import os

# Load environment variables
service_key = os.getenv('SERVICE_KEY_PATH')
bucket = os.getenv('BUCKET_NAME', 'data_ingestion_lake3')

# Load file paths from environment variables
sales_csv_path = os.getenv('SALES_CSV_PATH')
products_excel_path = os.getenv('PRODUCTS_XLSX_PATH')

# Load local datasets
df_sales = pd.read_csv(sales_csv_path)
df_products = pd.read_excel(products_excel_path)

# Load data from Google Sheets
df_promotions = pd.read_csv(
    'https://docs.google.com/spreadsheets/d/1QyjPdyp4DWlsPrwP14oOVyA6wYgr40z9H-f7-VWwf68/export?format=csv'
)

# Cleaning column names
df_sales.columns = df_sales.columns.str.strip().str.lower().str.replace(' ', '_')
df_products.columns = df_products.columns.str.strip().str.lower().str.replace(' ', '_')
df_promotions.columns = df_promotions.columns.str.strip().str.lower().str.replace(' ', '_')

# Filling missing numeric values
if 'quantity' in df_sales.columns:
    df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce').fillna(df_sales['quantity'].mean())

if 'price' in df_sales.columns:
    df_sales['price'] = pd.to_numeric(df_sales['price'], errors='coerce').fillna(df_sales['price'].mean())

if 'stock_level' in df_products.columns:
    df_products['stock_level'] = pd.to_numeric(df_products['stock_level'], errors='coerce').fillna(df_products['stock_level'].mean())

if 'discount_rate' in df_promotions.columns:
    df_promotions['discount_rate'] = pd.to_numeric(df_promotions['discount_rate'], errors='coerce').fillna(df_promotions['discount_rate'].mean())

# Removing duplicates
df_sales.drop_duplicates(inplace=True)
df_products.drop_duplicates(inplace=True)
df_promotions.drop_duplicates(inplace=True)

# Exporting datasets to Google Cloud Storage as Parquet
df_sales.to_parquet(
    f'gs://{bucket}/sales.parquet',
    index=False,
    storage_options={'token': service_key}
)

df_products.to_parquet(
    f'gs://{bucket}/products.parquet',
    index=False,
    storage_options={'token': service_key}
)

df_promotions.to_parquet(
    f'gs://{bucket}/promotions.parquet',
    index=False,
    storage_options={'token': service_key}
)
