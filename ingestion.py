import os
import csv
import psycopg2
from psycopg2 import OperationalError 
import re
from datetime import datetime


# Set variables
folder_path = ".\external-funds"
table_name_raw = "ext_funds_raw"

# print(os.getcwd())

# Using postgres 18
# Replace these with your actual database details 
DB_NAME = "gic"  # Default database name
DB_USER = "postgres"  # Your superuser or specific app user
DB_PASSWORD = "1234" 
DB_HOST = "localhost" # Or the IP address if remote
DB_PORT = "5433"      # Default port

"""
For Production :

1. Database setup - need to have a database either in the cloud or on-prem that can 
handle production scale data ingestion. 

2. With Database Connection String/Secrets - The database credentials (username, password, host, port) 
cannot be hardcoded. They must be managed securely using a secrets manager 
(e.g., Airflow Connections, AWS Secrets Manager).

3. Orchestrator (e.g., Apache Airflow) - Code to be arranged for scheduled execution, 
dependency management (e.g., run SQL after CSV is loaded), monitoring, and retry logic.

4. Containerization (e.g., Docker) - Packaging Python code, dependencies, and system configuration 
into an immutable unit ensures it runs identically on any server (your dev machine, staging, production).

5. Idempotency - The solution must be designed so that running it multiple times 
(due to retries or manual rerun) does not result in duplicate or incorrect data. 
This often involves using a staging table, transaction blocks, or an UPSERT (merge) strategy in SQL.

6. Data Validation/Schema Checks - Logic to ensure the input CSVs meet expected schema, 
data types, and constraints before loading into the database. 
Handling bad records (quarantining them or alerting).

7. Error Handling (Try/Except) - Implementing robust try...except blocks in Python, 
specifically around file I/O, network calls, and database transactions, to provide clear, 
actionable error messages instead of crashing the task.

8. SQL Separation - Move the raw .sql file logic into a separate, 
parameterized template (e.g., a .sql file read by your Python code) 
so it can be managed and versioned independently of the Python logic.

"""



def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Establishes a connection to the PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return conn


# Call the function to connect
# connection = create_connection(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)


def create_table_if_not_exist(table_name, connection):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS gic.public.{table_name} (
        FINANCIAL_TYPE VARCHAR(255) NOT NULL,
        SYMBOL VARCHAR(50) NOT NULL,
        SECURITY_NAME VARCHAR(255) NOT NULL,
        SEDOL VARCHAR(255),
        PRICE FLOAT,
        QUANTITY FLOAT,
        REALISED_PL FLOAT,
        MARKET_VALUE FLOAT NOT NULL,
        FILENAME VARCHAR(255) NOT NULL
    );
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table checked/created.")
    except Exception as e:
        connection.rollback()
        print(f"Error creating table: {e}")


def clean_commas_in_csv_folder(folder_path):
    # Code to cleane up some comma in somg of the cells
    # E.g. [..., Tapestry, Inc. , ...]
    # Out would be in a newly created "_cleaned" folder where the cleaned csv are stored
    os.makedirs(folder_path + "/" + "_cleaned", exist_ok=True)
    folder_path_cleaned = folder_path + "/" + "_cleaned"

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            input_path = os.path.join(folder_path, filename)
            clean_path = os.path.join(folder_path_cleaned, filename)

            with open(input_path, 'r', newline='', encoding='utf-8') as infile, \
                 open(clean_path, 'w', newline='', encoding='utf-8') as outfile:
                
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                for row in reader:
                    # Replace comma inside each cell (except delimiter commas)
                    cleaned_row = [cell.replace(",", "").replace("\n", "") for cell in row]
                    # Write cleaned rows into a new csv in the "_cleaned" folder
                    writer.writerow(cleaned_row)

            print(f"Cleaned commas in {filename}")

    return folder_path_cleaned


# cleaned up some comma
# E.g. [..., Tapestry, Inc. , ...]
# folder_path_cleaned = clean_commas_in_csv_folder(folder_path)


def to_float_or_none(s):
    # Change cell type to None if blank
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def load_csv_files_to_postgres(csv_folder, table_name, connection):
    # Load csv to postgress tables
    # Load filename as part of the row so as to extract portflio and reporting date
    cur = connection.cursor()
    
    # Clear existing data for idempotency
    cur.execute(f"TRUNCATE TABLE {table_name};")
    connection.commit()

    for filename in os.listdir(csv_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(csv_folder, filename)
            with open(filepath, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    # Add filename as the last column value
                    cleaned_row = list(row)
                    cleaned_row[4] = to_float_or_none(cleaned_row[4])  # PRICE
                    cleaned_row[5] = to_float_or_none(cleaned_row[5])  # QUANTITY
                    cleaned_row[6] = to_float_or_none(cleaned_row[6])  # REALISED_PL
                    cleaned_row[7] = to_float_or_none(cleaned_row[7])  # MARKET_VALUE
                    query = f"INSERT INTO {table_name} (FINANCIAL_TYPE, SYMBOL, SECURITY_NAME, SEDOL, PRICE, QUANTITY, REALISED_PL, MARKET_VALUE, FILENAME) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    # Line below that insert filename as well as the content of the csv
                    cur.execute(query, (*cleaned_row, filename))
                    print(f"Loaded {filename}")

    cur.close()
    connection.commit()
    connection.close()

# load_csv_files_to_postgres(folder_path, table_name_raw, connection)

def pipeline(table_name, csv_folder):
    connection = create_connection(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    create_table_if_not_exist(table_name, connection)
    folder_path_cleaned = clean_commas_in_csv_folder(csv_folder)
    load_csv_files_to_postgres(folder_path_cleaned, table_name, connection)
    print("-----------pipeline loaded---------")


# pipeline(table_name_raw, folder_path)

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure the folder exists for this local test
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' not found. Please create it and add CSV files.")
    else:
        connection = create_connection(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
        if connection:
            pipeline(table_name_raw, folder_path)
            connection.close()