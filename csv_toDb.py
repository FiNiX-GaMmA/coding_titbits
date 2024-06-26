import pandas as pd
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read the CSV file and get the schema
df = pd.read_csv('hour.csv', nrows=1)
schema = df.dtypes.to_dict()

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=os.getenv("PORT"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )
    print("Connected to the database")

    # Create a cursor object
    cur = conn.cursor()

    # Create the table based on the schema
    create_table_query = "CREATE TABLE IF NOT EXISTS hourly_rental_bikes ("
    for column, dtype in schema.items():
        if dtype == 'object':
            column_type = 'TEXT'
        elif dtype == 'int64':
            column_type = 'INTEGER'
        elif dtype == 'float64':
            column_type = 'REAL'
        elif dtype == 'datetime64[ns]':
            column_type = 'TIMESTAMP'
        else:
            column_type = 'TEXT'
        create_table_query += f"{column} {column_type}, "
    create_table_query = create_table_query[:-2] + ")"
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully!")

    # Read the entire CSV file and insert data into the table
    df = pd.read_csv('hour.csv')
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    insert_query = f"INSERT INTO hourly_rental_bikes ({', '.join(schema.keys())}) VALUES ({', '.join(['%s']*len(schema))})"
    cur.executemany(insert_query, rows)
    conn.commit()
    print(f"{len(rows)} rows inserted successfully!")

    cur.close()
    conn.close()

except (Exception, Error) as error:
    print("Error connecting to the database:", error)
