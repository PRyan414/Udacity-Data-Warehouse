import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function copies data from buckets into 'events' and 'songs' staging tables."""
    for query in copy_table_queries:
        try:
            print(f"Copying data into: {query}.")
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            if conn:
                conn.rollback()

def insert_tables(cur, conn):
    """This function inserts data into star schma tables from staging tables."""
    for query in insert_table_queries:
        try:
            cur.execute(query)
            print(f"Query executed successfully: {query}.")
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            if conn:
                conn.rollback()

def main():
    """Function connects to cluster and executes loading of staging tables followed by insertion into star schema tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
    CLUSTER_DB_USER = config.get("CLUSTER", "user")
    CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
    CLUSTER_DB_PORT = config.get("CLUSTER", "port")
    CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

    conn_string = f"dbname={CLUSTER_DB_NAME} user={CLUSTER_DB_USER} password={CLUSTER_DB_PASSWORD} host={CLUSTER_DB_ENDPOINT} port={CLUSTER_DB_PORT}"

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    try:
        load_staging_tables(cur, conn)
    except Exception as e:
        print(f"Error loading staging tables: {e}")
        if conn:
            conn.rollback()
        clear

    try:
        insert_tables(cur, conn)
    except Exception as e:
        print(f"Error with inserting tables: {e}")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
