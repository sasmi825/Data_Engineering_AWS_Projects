# Sparkify Data Warehouse On AWS Redshift

## 1. Project Overview

#### Context:

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.

#### Purpose:

This project addresses Sparkify's need by building an ***ETL pipeline*** that migrates their data onto a clud based data warehouse using ***Amazon Redshift***

## 2. Pipeline Architecture

The ETL pipeline is designed in a 3 stage process

1. **Data Staging**: 

      Raw JSON data (song metadata and user activity logs) is copied directly from Amzon S3 into dedicated staging tables within Redshift. This step is efficient for bulk loading and acts as a temporary holding area for raw data. 
        
2. **Data Transformation and Loading**:
    
      Data from satging tables is then transaformed and loaded into a ***star schema*** composed of a central fact table and in our case 4 dimension tables. This transformation involves cleaning, joining and aggregating data to fit the analytical model.
      
3. **Data Analysis**: 

      Once loaded into the star schema, the data is ready for fast and complex analytical queries,supports the Sparkify's business needs.

## 3. Dataset

Data is hosted on Amazon S3 and comes from two sources(datasets):

- **Song Data**: metadata about songs and artists.
                `s3://udacity-dend/song_data`
- **Log Data**: user activity logs such as songplays.
                `s3://udacity-dend/log_data`
                `s3://udacity-dend/log_json_path.json`

## 4. Schema Design 
The chosen schema is ***star schema***, which is a highly optimized for analytical querying and widely used in data warehousing. It consists of 
- **Fact Table** 
    - ***Songplay***(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                   Each record in this table is an instance of user playing a song.
        
    
- **Dimension Tables**
    - ***User***(user_id, first_name, gender, level)
                Stores information about the users.
    - ***Song***(song_id, title, artist_id, year, duration)
                Stores metadata about each song
    - ***Artist***(artist_id, name, location, latitude, longitude)
                Stores information about the artists in Sparkify's music library.
    - ***Time_table***(start_time, hour, day, week, month, year, weekday)
                Breaks down timestamps from song play events for time-based analysis.

## 5. File Descriptions

**create_tables.py**: This python script handles connecting to Redshift cluster, Dropping all tables and recreating all database tables based on schema definitions written in 'sql_queries.py'

**etl.py**: This is the core ETL pipeline script.It handles the entire data loading process:

            1. Connects to the Redshift cluster
            2. Executes 'COPY' commands to load raw song and log data from S3 into the staging_songs and staging_events tables.
            3. Executes 'INSERT' commands to transform and load data from the staging tables into final songplay, user, song, artist and time table, as per the star schema.
            4. Finally, it runs a set of analytic queries that display the row counts of all tables, confirming the ingestion of data to be successful.
            
**dwh.cfg**: Configuration file stores sensitive credentials and connection details (Amazon Redshift cluster endpoint, IAM role ARN, and S3 data paths).

**sql_queries.py**: Stores all SQL queries such as create, insert, copy, analyze and drop.


## 6. How To Run

### 6.1 Configure AWS Cloud Resources

Before running the python scripts, I set up the necessary AWS resources.

1. **Create Security Group in Amazon EC2**:
            - Navigate to the EC2 service in your AWS Management Console.
            - In the left hand navigation pane under "Network and security", select "Security Groups".
            - Click on Create Security Group, and fill the following details:
                    - **Security Group Name**: 'myRedshiftSecurityGroup'
                    - **Description**: Authorise Redshuft Cluster Access
                    - In Inbound Rules, add new rule
                             - Type: 'Custom TCP'
                             - Port range: 5439 (This is default for Redshift)
                             - Source: 'Anywhere-IPv4' (0.0.0.0/0) - Not recommended used only for course purposes
                    - Leave other fields to default values and click create security group.
                    
2. **Create an IAM Role for Redshift**:
            - Go to the IAM service in AWS Management Console
            - In the left-hand navigation pane,under "Access-management", select "roles"
            - Select "Create Role", and enter the following details
                     - **Trusted entitytype**: Keep default "AWS service"
                     - **Use case**: Search for and select "Redshift". Then choose 'Redshift-Customizable'.
                     - Click Next, Add Permission Policies:
                              - Search for and select 'AmazonS3ReadOnlyAccess'
                              - Search for and select 'AmazonRedshiftAllCommandsFullAccess' (this allows Redshift to perform various operations including 'Copy')
                     - Click next, give **Role Name**: 'myRedshiftRole'
                     - Review and click "Create Role"
             - Once the role is created click on the name to view the details. **Copy the Role ARN** We will need it later for 'dwh.cfg'.
            
3. **Create an Amazon redshift Cluster**: 
            - Go to the Amazon Redshift Service in the AWS Management Console.
            - In the left hand navigation pane, under clusters, Click **Clusters**
            - Click **Create Cluster**
            - Fill the following details
                      - **Cluster identifier**: 'redshift-cluster1'
                      - **Node type**: 'ra3.xlplus'
                      - **Number of Nodes**: '1'
                      - **Admin user name**: 'awsuser'
                      - **Admin password**: Manually enter a strong password (e.g., `Password01M`)**Copy the credentials**
                      - **Associate IAM roles**: Attach the IAM role you just created ('myRedshiftRole').
                      - **Network and security**:
                              - **VPC security groups**: Select the security group you created ('myRedshiftSecurityGroup').
                              - **Publicly Accessible**: Turn On
                      - Leave the rest settings to default and click **Create Cluster**
             - Wait for cluster status to change to Available, click on cluster name and check the properties. **Copy the Endpoint** and **port**


### 6.2 Configure 'dwf.cfg'

Now that the cluster is ready we go to the coding part.

Open the 'dwh.cfg' file and update with the copied values and specific details:

```
[CLUSTER] 

HOST=redshift-cluster-endpoint.redshift.amazonaws.com
DB_NAME=dev
DB_USER=awsuser
DB_PASSWORD=your_admin_password
DB_PORT=5439

[IAM_ROLE]
ARN=arn:aws:iam::xxxxxxxxxxxx:role/your-redshift-iam-role

[S3]
LOG_DATA=s3://udacity-dend/log_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data


```
             
### 6.3 Run the ETL Pipeline

Now that the AWS Resources are configured and dwh.cfg is updated , you can run python scripts from the terminal.

1. **Set up Tables**:
        This script will connect to the redshift cluster, drop any existing tables , and then create all the staging, fact, dimenson tables with the defined schema. 
        - Run command in Bash:
        
                ```
                python create_tables.py
                
                ```

2. **Run the ETL process**:
        This script will perform the data loading:
                - It will copy raw data from S3 into your staging tables.
                - It will then transform and insert this data into your final star schema tables.
                - Finally, it will print the row counts for each of the tables, confirming thhat data has been successfully loaded.
                - Run command in Bash:
        
                ```
                python etl.py
                
                ```
                
## 7. Performing Analytical Queries

Once the ETL pipeline has successfully loaded data into the Redshift data warehouse, Sparkify's team can run queries to gain insights. 

## 8. Deleting AWS Resources:

To avoid incurring cost, delete the redshift cluster and associated resources once completed the project and testing.

1. **Delete Redshift Cluster**: 
        - Navigate to the cluster 
        - Click "Actions" , Select "Delete cluster"
        - Uncheck create final snapshot unless you need one.
        - Confirm deletion

2. **Delete IAM Role**: 
        - Navigate to the IAM service in AWS Console
        - Under "Roles", find and select 'myRedshiftRole'
        - Click "Delete Role"

3. **Delete Security Group**:
        - Navigate to EC2 service in AWS Console
        - Under "Security Groups", find and select 'myRedshiftSecurityGroup'.
        - Click "Actions" and click "Delete security group"
        
By following these steps, we can ensure a clean shutdown of AWS Environment.

## 9. Sample Output

home root$ python create_tables.py
home root$ python etl.py
Successfully Connected To Redshift.

--- Loading Staging Tables ---

--- Inserting Data Into Tables ---

--- Analyzing Tables ---

SELECT COUNT(*) FROM song
[14896]
SELECT COUNT(*) FROM songplay
[7161]
SELECT COUNT(*) FROM artist
[10025]
SELECT COUNT(*) FROM user
[104]
SELECT COUNT(*) FROM time_table
[8023]
SELECT COUNT(*) FROM staging_events
[8056]
SELECT COUNT(*) FROM staging_songs
[14896]

Connection closed. ETL process completed.

home root$
        