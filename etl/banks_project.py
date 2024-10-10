# Code for ETL operations on Country-GDP data

import numpy as np
import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import xml.etree.ElementTree as ET

# Init
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path="./Largest_banks_data.csv"
db_name="Banks.db"
table_name="Largest_banks"
log_file = "code_log.txt"
table_attribs = ["Bank Name","MC_USD_Billion"]

# Log file
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')
        
# Extract the data
def extract(url,table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    #Loading the webpage for Webscraping
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    #Scraping of required information
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0]
            data_dict = {"Bank Name": bank_name,"MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1],ignore_index=True)
            
    return df

    
# Tramsfor the data
def transform(df, csv_path):
    
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # Get the exchange rates
    file = "exchange_rate.csv"
    exchange_rate = pd.read_csv(file)
    gbp_rate = exchange_rate[exchange_rate['Currency'] == 'GBP']['Rate'].values[0]
    eur_rate = exchange_rate[exchange_rate['Currency'] == 'EUR']['Rate'].values[0]
    inr_rate = exchange_rate[exchange_rate['Currency'] == 'INR']['Rate'].values[0]
    
    
    # Convert 'MC_USD_Billion' to numeric if it is not already
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    
    # Calculate the Market Cap in other currencies
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * gbp_rate, 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * eur_rate, 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * inr_rate, 2)
    
    print(df)
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress(f'Data successfully saved to {output_path}')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress(f'Data successfully loaded into {table_name} table in the database.')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    log_progress(f'Executing query: {query_statement}')
    try:
        result = pd.read_sql_query(query_statement, sql_connection)
        print(f"Query Statement: {query_statement}\n")
        print("Query Output:")
        print(result)
        log_progress('Query executed successfully.')
    except Exception as e:
        log_progress(f'Error executing query: {str(e)}')
        print(f'Error executing query: {str(e)}')


# Main script execution
if __name__ == "__main__":
    # Test
    log_progress("Initiating ETL process")
    
    log_progress('Initiating Extraction process')
    extracted_df = extract(url, table_attribs)
    log_progress('Extraction process ended')
    
    log_progress('Initiating Transformation process')
    transformed_df = transform(extracted_df, csv_path)
    log_progress('Transformation process ended')
    
    log_progress('Initiating Load process to CSV')
    load_to_csv(transformed_df, csv_path)
    log_progress('Load process to CSV ended')
    
    log_progress('Initiating Load process to Database')
    conn = sqlite3.connect(db_name)
    load_to_db(transformed_df, conn, table_name)
    log_progress('Load process to Database ended')
    
    log_progress('Running a sample query on the Database')
    sample_query = f"SELECT * FROM {table_name} WHERE MC_GBP_Billion > 50"
    run_query(sample_query, conn)
    sample_query = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
    run_query(sample_query, conn)
    sample_query = f"SELECT 'Bank Name' from {table_name} LIMIT 5"
    run_query(sample_query, conn)
    
    log_progress('ETL process completed successfully')

