import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries


# Function to copy data from buckets into 'events' and 'songs' staging tables; LIST: "copy_table_queries" list from sql_queries.py
def load_staging_tables(cur, conn):
    # try and except code to catch errors when loading staging tables.
    for query in copy_table_queries:
        try:
            print(f"Copying data into: {query}.")
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            if conn:
                conn.rollback()  # Roll back the transaction on error


# Function to insert data into star schema tables from staging tables; 
# LIST: "insert_table_queries" (from sql_queries.py); similar try/except format as above.
def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            print(f"Query executed successfully: {query}.")
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            if conn:
                conn.rollback()  # Roll back the transaction on error

# Function, a) reads config file parameters, establishes connection with cluster, creates a cursor to execute queries, then first executes "load_staging_tables" fxn, followed by "insert_tables" fxn.  Both fxns have "cur" and "conn" as arguments.
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
    CLUSTER_DB_USER = config.get("CLUSTER", "user")
    CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
    CLUSTER_DB_PORT = config.get("CLUSTER", "port")
    CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

    # Connect to Redshift and set up tables
    # Connection string for use below.
    conn_string = f"dbname={CLUSTER_DB_NAME} user={CLUSTER_DB_USER} password={CLUSTER_DB_PASSWORD} host={CLUSTER_DB_ENDPOINT} port={CLUSTER_DB_PORT}"

    # Create connection and establish cursos.
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    # First execute loading of staging tables.
    try:
        load_staging_tables(cur, conn)
    except Exception as e:
        print(f"Error loading staging tables: {e}")
        if conn:
            conn.rollback()
        clear
    # Next, insert data into tables.
    try:
        insert_tables(cur, conn)
    except Exception as e:
        print(f"Error with inserting tables: {e}")
        
    # When finished, close the cursor and the connection.
    cur.close()
    conn.close()

# Code to call function above.
if __name__ == "__main__":
    main()