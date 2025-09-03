-----

### Data Pipelines with Apache Airflow

This project implements an ETL (Extract, Transform, Load) data pipeline for Sparkify, a music streaming company, using Apache Airflow. The pipeline automates the processing of user activity and song metadata from Amazon S3 and loads the transformed data into a data warehouse in Amazon Redshift. It is designed to be dynamic, easily monitored, and supports backfills.

#### Project Overview

The core of this project is a Directed Acyclic Graph (DAG) that defines the workflow for processing data. The pipeline performs the following key steps:

  * **Staging Data**: Raw JSON logs of user activity and song metadata are staged from S3 into Redshift staging tables.
  * **Loading Fact and Dimension Tables**: Data from the staging tables is used to populate a central fact table (**songplays**) and four dimension tables (**users, songs, artists, time**) in the data warehouse.
  * **Data Quality Checks**: After the ETL process, a series of data quality checks are run to ensure the integrity of the loaded data.

The DAG's dependencies are configured to ensure a logical and coherent data flow:

  * The pipeline begins with a **Begin\_execution** task.
  * It then branches into two parallel staging tasks: **Stage\_events** and **Stage\_songs**.
  * Once both staging tasks are complete, the **Load\_songplays\_fact\_table** task runs.
  * After the fact table is loaded, four dimension tables are loaded in parallel: **Load\_artist\_dim\_table, Load\_song\_dim\_table, Load\_time\_dim\_table,** and **Load\_user\_dim\_table**.
  * After all dimension tables are loaded, the **Run\_data\_quality\_checks** task executes.
  * The pipeline concludes with a **Stop\_execution** task.

-----

#### Project Components

This project is built on a template that includes the necessary files for a complete Airflow solution. The key components are:

  * **Custom Operators**: The project uses four custom Airflow operators to perform specific ETL tasks. These operators are designed to be reusable and flexible, leveraging Airflow's built-in functionalities like hooks and connections.
      * `StageToRedshiftOperator`: Loads any JSON-formatted file from S3 into a Redshift table. It creates and runs a SQL `COPY` statement dynamically based on parameters and can handle timestamped files for backfills.
      * `LoadFactOperator`: Populates the **songplays** fact table using an SQL statement from a helper class. This operator is designed for append-only functionality, as fact tables are typically massive.
      * `LoadDimensionOperator`: Fills the dimension tables. It takes an SQL statement as input and can be configured to use either an append-only or truncate-insert pattern.
      * `DataQualityOperator`: Runs data integrity checks using one or more SQL test cases and compares the results to an expected outcome. If a test fails, it raises an exception, causing the task to fail.
  * **SQL Helper Class**: A Python class containing all the necessary SQL transformation statements. The custom operators use this class to execute the ETL logic.

-----

#### Getting Started

##### Prerequisites

To set up and run this project, you will need:

  * An **AWS** account with an IAM user configured.
  * An **Amazon Redshift Serverless** workgroup configured in your AWS account.
  * An Amazon S3 bucket to store the project data.

##### Setup and Configuration

1.  **Data Setup**: Copy the provided datasets from the Udacity S3 bucket (`s3://udacity-dend/log_data` and `s3://udacity-dend/song-data`) into your own S3 bucket. This ensures Redshift can access the data, as Redshift can only access S3 buckets in the same AWS region.
2.  **Airflow Connections**: Configure the following connections in your Airflow instance to connect to AWS and Redshift:
      * `aws_credentials`: For connecting to S3.
      * `redshift`: For connecting to your Redshift data warehouse.
3.  **DAG Configuration**: The project template's DAG is provided with tasks but without dependencies. You must configure the task dependencies to create a coherent data flow as shown in the project overview. You also need to set the following default parameters for the DAG:
      * No dependencies on past runs.
      * Retries on failure: 3 times.
      * Retry delay: 5 minutes.
      * Catchup: turned off.
      * Email on retry: turned off.
4.  **Operator Implementation**: The final step is to complete the four empty operator templates (`stage_redshift.py`, `load_fact.py`, `load_dimensions.py`, and `data_quality.py`) to make them functional. Implement the logic for staging data, loading fact and dimension tables, and performing data quality checks as described above.

-----

### Running the DAG and Verifying Results 🚀

#### Running the DAG

To run the ETL pipeline, ensure your Airflow environment is set up and configured correctly, including the necessary AWS and Redshift connections. Once configured, you can trigger the DAG from the Airflow UI:

1.  Navigate to the main **DAGs** page in the Airflow UI.
2.  Locate the `sparkify_etl` DAG and toggle the status to **ON**.
3.  Click the "Trigger DAG" icon (the play button) to manually start a new run.
4.  The DAG will begin execution according to its defined dependencies, highlighting tasks that are currently running, have succeeded, or have failed.

#### Checking for Correct Execution

After triggering the DAG, you can monitor its progress through the Airflow UI. The **Graph View** and **Tree View** provide a visual representation of the pipeline, showing the status of each task in real-time. This helps you quickly identify any tasks that fail and need attention.

#### Scripts for Verification

Once the DAG run completes successfully, you can verify the integrity of the data in your Redshift data warehouse using a SQL client. The following SQL scripts can be used to perform quick checks:

**1. Check Row Counts**
This script verifies that data has been loaded into each of the fact and dimension tables by counting the number of rows. A row count of 0 for any table would indicate a failure in the ETL process.

```sql
SELECT count(*) FROM songplays;
SELECT count(*) FROM users;
SELECT count(*) FROM songs;
SELECT count(*) FROM artists;
SELECT count(*) FROM time;
```

**2. Spot Check Data**
These queries allow you to inspect a small sample of the loaded data to ensure it appears in the correct format and contains the expected information.

```sql
-- Check sample data in the songplays fact table
SELECT * FROM songplays LIMIT 10;

-- Check sample data in the users dimension table
SELECT userid, first_name, last_name FROM users LIMIT 10;

-- Check sample data in the songs dimension table
SELECT title, artist_id FROM songs LIMIT 10;
```

**3. Data Integrity Check**
This query checks for potential issues, such as entries in the fact table (`songplays`) that don't have a corresponding record in a dimension table (e.g., `songs`). A result of `0` from this query indicates that there are no orphaned records and the data integrity is sound.

```sql
SELECT count(*)
FROM songplays sp
LEFT JOIN songs s ON sp.songid = s.song_id
WHERE s.song_id IS NULL;
```
