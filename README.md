# Udacity-Data-Warehouse
Data warehouse project for Course 3 of the Data Engineering Nano-Degree Course

## Purpose
The purpose of this project and the database it creates is an example of building an ETL pipeline for a fictional company called Sparkify, which is a music streaming startup.  The ETL pipeline created with python code shows the process of connecting to AWS S3, creating a user with specific policy rights and also creating and connecting to a Redshift cluster where the database is actually housed and from which it can be queried.  The pipeline ingests data from its original storage in S3 and copies data to staging tables that hold song and event data pertaining to all users of Sparkify's service.  In turn, this data is inserted in to a star schema database for quick and streamlined querying to garner analytics and insights that can help Sparkify improve its services and perhaps grow its business.

## ETL Pipeline
The ETL pipeline is comprised of the following python files, each created for a specific part of the process:
1. iac.py (aka "internet_as_code".py).  While not specifically called for by the project, this file contains code for checking if an AWS IAM role exists and if not creating one with the appropriate policies assigned.  It also has a function to create a Redshift cluster, messaging to alert a user when the cluster is available, and then notification that the user is in fact connected to the cluster.  iac.py is priimarily called upon by create_cluster.py
2. create_cluster.py: This code checks for and creates an IAM role if it does not exist, then creates a cluster and notifies the user when the cluster is available for subsequent scripts / pytyon code.
3. create_tables.py: This is a short python script containing simple functions to drop all tables imported from a list in sql_queries.py and a function to create those tables anew.
4. etl.py and sql_queries.py: *sql_queries.py* contains all of the code that drops and creates tables, as well as copies the data into staging tables and then inserts them in the star schema tables.  All of the code from sql_queries.py is imported into etl.py for use in specific functions.  *etl.py* contains functions to load data in to the staging tables and then copy that data into the star schema tables organized into fact (songplays) and dimension tables where it can be transformed as needed in the insertion process.  There are also queries built here that briefly demonstrate how data can be queried from the database.  These queries are called from the data_queries.py python file.
5. The ETL pipeline is constructed in a way that tables can easily be dropped and recreated to ensure data are updated and populated into tables correctly each time the pipeline is run.

## Star Schema
The star schema is organized by 'songplays', the primary fact table and data of interest and then connected to dimensional tables focused on users, songs, artists and time pertaininig to Sparkify users.  Tables can be easily joined and queried as needed based on specific types of enquiries.

## Running the Code:
1. Open a terminal in the directory containing all necessary files.
2. Run 'create_cluster.py'. When notified that the cluster is ready, proceed to step 3.
3. Run 'create_tables.py'.
4. When tables are created and populated with data, finally run 'data_queries.py' for a brief demonstration of the type of data that can be extracted using queries.
