import pandas as pd
import datetime
import os
import numpy as np
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from the .env file for Supabase configuration
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_EMAIL = os.getenv("SUPABASE_EMAIL")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configure logging to track the ETL process
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def authenticate_user():
    """Authenticate the user with Supabase using email and password."""
    try:
        auth_response = supabase.auth.sign_in_with_password({
            "email": SUPABASE_EMAIL,
            "password": SUPABASE_PASSWORD
        })
        if not auth_response:
            raise Exception("Authentication failed.")
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        raise

def log_step(step, status, error_message=None):
    """Log the progress of each step in the ETL process."""
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "step": step,
        "status": status,
        "error_message": error_message,
    }
    try:
        response = supabase.table("logs").insert(log_entry).execute()
        if response is None:
            logging.warning(f"Failed to log {step} step")
    except Exception as e:
        logging.error(f"Error logging step '{step}': {e}")

def clean_dataframe(df):
    """Clean the DataFrame by handling NaN, infinite, and non-numeric values appropriately."""
    if df is None:
        raise ValueError("Input DataFrame is None.")
    
    # Separate numeric columns and process them
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')  # Convert invalid entries to NaN
    # Replace infinite values with NaN in numeric columns
    df[numeric_columns] = df[numeric_columns].replace([np.inf, -np.inf], np.nan)
    # Replace all NaNs in the DataFrame with None (to handle insertion as NULL in databases)
    df = df.where(pd.notnull(df), None)
    return df

def fetch_data(url):
    """Fetch data from the specified URL and clean the DataFrame."""
    log_step("Fetching data from HDX", "In Progress")
    try:
        df = pd.read_csv(url)  # Read CSV data into a DataFrame
        df = clean_dataframe(df)  # Clean the DataFrame
        logging.info(f"Fetched DataFrame shape: {df.shape}")  # Log the shape of the DataFrame
        
        # Remove NaN Rows and drop the first row if necessary
        df = df.dropna()
        df.drop(index=0, inplace=True)  # This will remove the first row in place
        log_step("Fetching data from HDX", "Success")
        return df
    except Exception as e:
        log_step("Fetching data from HDX", "Error", str(e))
        raise

def create_dim_date(df):
    """Creates a date dimension table from the DataFrame."""
    dim_date = df[['date']].drop_duplicates().reset_index(drop=True)  # Select unique dates
    dim_date['year'] = dim_date['date'].dt.year  # Extract year
    dim_date['quarter'] = dim_date['date'].dt.quarter  # Extract quarter
    dim_date['month'] = dim_date['date'].dt.month  # Extract month
    dim_date['day'] = dim_date['date'].dt.day  # Extract day
    dim_date['weekday'] = dim_date['date'].dt.weekday  # Extract weekday
    dim_date['date_id'] = range(1, len(dim_date) + 1)  # Create a unique ID for each date
    return dim_date

def create_dim_market(df):
    """Creates a market dimension table from the DataFrame."""
    return df[['admin1', 'admin2', 'market', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True).assign(market_id=lambda x: x.index + 1)

def create_dim_commodity(df):
    """Creates a commodity dimension table from the DataFrame."""
    return df[['category', 'commodity']].drop_duplicates().reset_index(drop=True).assign(commodity_id=lambda x: x.index + 1)

def create_fact_price_data(df, dim_date, dim_market, dim_commodity):
    """Creates a fact table for price data by joining dimension tables."""
    date_mapping = dim_date[['date', 'date_id']]  # Map dates to their IDs
    fact_data = df.merge(date_mapping, on='date') \
                  .merge(dim_market, on=['admin1', 'admin2', 'market', 'latitude', 'longitude']) \
                  .merge(dim_commodity, on=['category', 'commodity'])
    
    # Rename columns to match the database schema
    fact_data = fact_data.rename(columns={
        'priceflag': 'price_flag',  
        'pricetype': 'price_type',  
        'usdprice': 'usd_price'     
    })
    
    # Return the relevant columns for the fact table
    return fact_data[['date_id', 'market_id', 'commodity_id', 'price', 'price_flag', 'price_type', 'currency', 'usd_price']]

def transform_data(df):
    """Transform the DataFrame into a star schema format."""
    log_step("Transforming data into star schema", "In Progress")
    try:
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')  # Convert date strings to datetime objects
        dim_date = create_dim_date(df)  # Create date dimension
        dim_market = create_dim_market(df)  # Create market dimension
        dim_commodity = create_dim_commodity(df)  # Create commodity dimension
        fact_price_data = create_fact_price_data(df, dim_date, dim_market, dim_commodity)  # Create fact table
        
        # Clean all tables
        dim_date = clean_dataframe(dim_date)
        dim_market = clean_dataframe(dim_market)
        dim_commodity = clean_dataframe(dim_commodity)
        fact_price_data = clean_dataframe(fact_price_data)
        
        # Convert date to ISO format string for consistency
        dim_date['date'] = dim_date['date'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        
        log_step("Transforming data into star schema", "Success")
        return dim_date, dim_market, dim_commodity, fact_price_data
    except Exception as e:
        log_step("Transforming data into star schema", "Error", str(e))
        raise

def load_data(table_name, data, max_retries=3, batch_size=1000):
    """Load data into the specified Supabase table."""
    log_step(f"Loading data into {table_name}", "In Progress")
    try:
        records = data.to_dict(orient="records")  # Convert DataFrame to a list of records
        
        try:
            response = supabase.table(table_name).upsert(records).execute()  # Upsert records into the table
            
            if response is None:
                raise Exception("No response")
        except Exception as e: 
            log_step(f"Loading data into {table_name}", "Error", str(e))
            raise
        log_step(f"Batch {len(records)} into {table_name}", "Success")
        
    except Exception as e:
        log_step(f"Loading data into {table_name}", "Error", str(e))
        raise

def main():
    """Main function to execute the ETL process."""
    try:
        authenticate_user()  # Authenticate the user with Supabase

        # HDX CSV URL
        csv_url = "/Users/stevenogwal/development/ml_eng/inflation_project/etl/data/wfp_food_prices_uga.csv"

        # Step 1: Fetch and transform data
        df = fetch_data(csv_url)  # Fetch data from the CSV file
        if df is None:
            raise ValueError("Fetched DataFrame is None.")
        
        # Clean the DataFrame
        df = clean_dataframe(df)

        # Transform the data into the required format
        dim_date, dim_market, dim_commodity, fact_price_data = transform_data(df)

        # Step 2: Load data into Supabase
        load_data("dim_date", dim_date)  # Load date dimension
        load_data("dim_market", dim_market)  # Load market dimension
        load_data("dim_commodity", dim_commodity)  # Load commodity dimension
        load_data("fact_price_data", fact_price_data)  # Load fact price data

    except Exception as e:
        log_step("ETL Process", "Error", str(e))
        logging.error(f"An error occurred in the ETL process: {e}")

if __name__ == "__main__":
    main()  # Execute the main function
