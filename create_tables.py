# running "iac_v2.py" code correctly.  In that file, add function to create, wait and connect to RS cluster
import configparser
import psycopg2
import boto3
import time
import json
from sql_queries import create_table_queries, drop_table_queries
from iac import check_iam_role_exists, create_iam_role, create_redshift_cluster, wait_for_cluster_available, check_cluster_availability

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
# REGION = config.get("S3", "REGION")
# role_name = config.get("DWH", "DWH_IAM_ROLE_NAME")
# role_arn = config.get("IAM_ROLE", "ARN")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
# DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
# DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
# DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
CLUSTER_DB_USER = config.get("CLUSTER", "user")
CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
CLUSTER_DB_PORT = config.get("CLUSTER", "port")
CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
redshift = boto3.client('redshift', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')

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

    # role_name = config.get("DWH", "DWH_IAM_ROLE_NAME")
    role_name = DWH_IAM_ROLE_NAME

    role_exists = check_iam_role_exists(role_name)

    if role_exists:
        print(f"Using existing IAM role: {role_name}")
        role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
        print(f"Role_ARN is: {role_arn}")
    else:
        create_iam_role(role_name, 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')


    create_redshift_cluster(role_arn)

    # Wait for the cluster to be available
    wait_for_cluster_available(DWH_CLUSTER_IDENTIFIER)

    # Connect to Redshift and set up tables
    conn_string = f"dbname={CLUSTER_DB_NAME} user={CLUSTER_DB_USER} password={CLUSTER_DB_PASSWORD} host={CLUSTER_DB_ENDPOINT} port={CLUSTER_DB_PORT}"

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()