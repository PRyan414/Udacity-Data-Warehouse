import configparser
import psycopg2
import logging
from sql_queries import data_queries_list

# Idea for this script: load queries (defined in sql_queries.py?) and execute in list form?

# Function, a) reads config file parameters, establishes connection with cluster, creates a cursor to execute queries, then executes...Both fxns have "cur" and "conn" as arguments.

# IDEA: WRITE PRINTING OF QUERY RESULTS INTO A FUNCTION TO BE IMPORTED;
# Capture queries in a list as with create, insert and copy functions, so you don't need to run them one at a time?
# Something like for query in list of queries > execute queries, capture results (own fxn?), print fxn: those would be the lines in the main function.


def data_discovery(cur, conn):
    for query in data_queries_list:
        cur.execute(query)
        print(f" The result of query \n {query} is: \n")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("\n")
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
    CLUSTER_DB_USER = config.get("CLUSTER", "user")
    CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
    CLUSTER_DB_PORT = config.get("CLUSTER", "port")
    CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

    # Connect to Redshift and set up tables
    conn_string = f"dbname={CLUSTER_DB_NAME} user={CLUSTER_DB_USER} password={CLUSTER_DB_PASSWORD} host={CLUSTER_DB_ENDPOINT} port={CLUSTER_DB_PORT}"
    
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    try:
        data_discovery(cur, conn)
    except Exception as e:
        print(f"Error with query {query}: {e}")
        if conn:
            conn.rollback()
        clear

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()