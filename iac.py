# code for creating IAM role, assigning policy, connecting to redshift cluster...
import boto3
import configparser
import psycopg2
import boto3
import time

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
CLUSTER_DB_NAME = config.get("CLUSTER", "dbname")
CLUSTER_DB_USER = config.get("CLUSTER", "user")
CLUSTER_DB_PASSWORD = config.get("CLUSTER", "password")

# Create IAM and Redshift clients.
iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
redshift = boto3.client('redshift', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')

# Function to check if IAM role exists
def check_iam_role_exists(role_name):
    try:
        iam.get_role(RoleName=role_name)
        print(f"IAM role {role_name} already exists.")
        return True
        
    except iam.exceptions.NoSuchEntityException:
        print(f"IAM role {role_name} does not exist.")
        return False
        
    except Exception as e:
        print(f"Error checking IAM role: {e}")
        return False

# Function to create IAM role if it doesn't exist.
def create_iam_role(role_name, policy_arn):
    # iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
    
    # Create the IAM role
    try:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument='''{
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "redshift.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }]
            }'''
        )
        print(f"Role {role_name} created.")
    except Exception as e:
        print(f"Error creating role: {e}")
        return
    
    # Attach the policy
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        print(f"Policy {policy_arn} attached to role {role_name}.")
    except Exception as e:
        print(f"Error attaching policy: {e}")

# Function to create Redshift cluster
def create_redshift_cluster(role_arn):
    try:
        print("Creating Redshift cluster...")
        redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=CLUSTER_DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=CLUSTER_DB_USER,
            MasterUserPassword=CLUSTER_DB_PASSWORD,
            IamRoles=[role_arn]
        )
        print("Redshift cluster creation initiated.")
    except Exception as e:
        print(f"Error creating cluster: {e}")

# Function to wait for the Redshift cluster to become available
def wait_for_cluster_available(cluster_identifier):
    while True:
        try:
            cluster_info = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
            cluster_status = cluster_info['Clusters'][0]['ClusterStatus']
            
            if cluster_status == 'available':
                print("Cluster is available.")
                return
            else:
                print(f"Cluster status: {cluster_status}. Waiting for the cluster to become available...")
                time.sleep(30)  # Wait for 30 seconds before checking again
        except Exception as e:
            print(f"Error checking cluster status: {e}")
            time.sleep(30)

def check_cluster_availability(cluster_identifier):
    try:
        cluster_info = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
        clluster_status = cluster_info['Clusters'][0]['ClusterStatus']
        
        if cluster_status == 'available':
            print("Cluster already available.")
            return

    except Exception as e:
            print(f"Error checking cluster status: {e}")

