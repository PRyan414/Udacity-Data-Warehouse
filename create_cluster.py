import configparser
import psycopg2
import boto3
import time
import json
from iac import check_iam_role_exists, create_iam_role, create_redshift_cluster, wait_for_cluster_available, check_cluster_availability

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
CLUSTER_DB_USER = config.get("CLUSTER", "user")
CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")
CLUSTER_DB_PORT = config.get("CLUSTER", "port")
CLUSTER_DB_ENDPOINT = config.get("CLUSTER", "host")

iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
redshift = boto3.client('redshift', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')

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

if __name__ == "__main__":
    main()