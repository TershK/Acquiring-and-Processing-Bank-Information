# Acquiring-and-Processing-Bank-Information
# %% [markdown]
# # Final Project: Acquiring and Processing Information on World's Largest Banks
# **Goal:** Extract list of top 10 largest banks by market cap, transform currencies, and load to CSV/SQL.

# %% [markdown]
# ### Step 0: Create the Exchange Rate CSV
# This creates the local file required for the transformation step.

# %%
import pandas as pd

exchange_data = {
    'Currency': ['EUR', 'GBP', 'INR'],
    'Rate': [0.93, 0.8, 82.9]
}
df_ex = pd.DataFrame(exchange_data)
df_ex.to_csv('exchange_rate.csv', index=False)
print("exchange_rate.csv created successfully.")

# %% [markdown]
# ### Step 1: Import Libraries and Define ETL Functions

# %%
import numpy as np
import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    log_progress("Starting extraction...")
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            # Targeting the 'Bank name' and 'Market cap' columns
            bank_name = col[1].get_text().strip()
            # Remove any non-numeric chars like commas or dollar signs
            market_cap = float(col[2].get_text().strip().replace(',', ''))
            curr_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            df = pd.concat([df, pd.DataFrame([curr_dict])], ignore_index=True)
    
    return df

def transform(df, csv_path):
    log_progress("Starting transformation...")
    exchange_rate = pd.read_csv(csv_path)
    rates = exchange_rate.set_index('Currency').to_dict()['Rate']
    
    # Mathematical transformation: USD * Rate
    df['MC_GBP_Billion'] = [np.round(x * rates['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * rates['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * rates['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    log_progress(f"Data saved to CSV: {output_path}")

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress(f"Data loaded to Database table: {table_name}")

def run_query(query_statement, sql_connection):
    print(f"Executing: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    log_progress(f"Query executed: {query_statement}")

# %% [markdown]
# ### Step 2: Execute the ETL Process

# %%
# Parameters
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'

# Execution
log_progress('Preliminaries complete. Initializing ETL process')

# Extract
df = extract(url, table_attribs)
print("--- Question 2 Output (Extraction) ---")
print(df.head(10)) 

# Transform
log_progress('Data extraction complete. Initializing Transformation process')
df = transform(df, csv_path)

# Load CSV
log_progress('Data transformation complete. Initializing Loading process')
load_to_csv(df, output_path)

# Load DB
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

# Queries (Question 4 & 5 Verification)
run_query('SELECT * FROM Largest_banks', sql_connection)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
run_query('SELECT Name from Largest_banks LIMIT 5', sql_connection)

log_progress('Process Complete.')
sql_connection.close()

# %% [markdown]
# ### Step 3: Check Log Entries (Question 5)
# %%
with open('code_log.txt', 'r') as f:
    print(f.read())
