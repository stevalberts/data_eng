import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Database connection details using SQLAlchemy
engine = create_engine('postgresql+psycopg2://stevenogwal:""@localhost:6432/PracProj')

# Load the CSV file using pandas and select the necessary columns
csv_file_path = 'DimProduct.csv'
df = pd.read_csv(csv_file_path)

# Upload the data into the database using SQLAlchemy engine
with engine.connect() as connection:
    df.to_sql('dimproduct', connection, if_exists='replace', index=False)

# Load another CSV file (DimDate.csv)
csv_file_path = 'DimDate.csv'
df = pd.read_csv(csv_file_path)

# Upload the data into the database
with engine.connect() as connection:
    df.to_sql('dimdate', connection, if_exists='replace', index=False)

# Load another CSV file (DimCustomer.csv)
csv_file_path = 'DimCustomerSegment.csv'
df = pd.read_csv(csv_file_path)

# Upload the data into the database
with engine.connect() as connection:
    df.to_sql('dimcustomersegment', connection, if_exists='replace', index=False)
    
# Load another CSV file (FactSales.csv)
csv_file_path = 'FactSales.csv'
df = pd.read_csv(csv_file_path)

# Upload the data into the database
with engine.connect() as connection:
    df.to_sql('factsales', connection, if_exists='replace', index=False)

# Close the connection (SQLAlchemy handles this internally but it's good practice)
engine.dispose()

print("Data successfully inserted into the database!")
