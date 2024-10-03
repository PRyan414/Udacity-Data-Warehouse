# Code with functions to drop all tables in a given list, and also create tables from a given list.
import configparser
import psycopg2
import boto3
import time
import json
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

# Config file information needed to connect to the Redshift cluster already created.
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
CLUSTER_DB_USER = config.get("CLUSTER", "user")
CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
CLUSTER_DB_PORT = config.get("CLUSTER", "port")
CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

# Function to drop tables imported in the list "drop_table_queries" from sql_queries.py
# For each query/table in the list, code to drop each table is run.
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift and set up tables
    # Connection string to use in connection command.
    conn_string = f"dbname={CLUSTER_DB_NAME} user={CLUSTER_DB_USER} password={CLUSTER_DB_PASSWORD} host={CLUSTER_DB_ENDPOINT} port={CLUSTER_DB_PORT}"

    # Create connection and establish a cursor to execute commands on the luster
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    # Run each of the functions above, sequentially.
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the cursor connection.
    conn.close()


if __name__ == "__main__":
    main()