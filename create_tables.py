"""Code with functions to drop all tables in a given list, and also create tables from a given list."""
import configparser
import psycopg2
import boto3
import time
import json
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
CLUSTER_DB_USER = config.get("CLUSTER", "user")
CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
CLUSTER_DB_PORT = config.get("CLUSTER", "port")
CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

def drop_tables(cur, conn):
    """Function to drop tables from database if they are found to exist."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
        """Function creates tables for database as defined in sql_queries.py."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Executes functions drop_tables followed by crete_tables as precursors to etl.py."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn_string = f"dbname={CLUSTER_DB_NAME} user={CLUSTER_DB_USER} password={CLUSTER_DB_PASSWORD} host={CLUSTER_DB_ENDPOINT} port={CLUSTER_DB_PORT}"

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
