import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, analyze_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 buckets into the staging tables in Redshift.
    This function iterates through a list of COPY queries to stage raw song and log data.
    """
    
    print("\n --- Loading Staging Tables --- \n")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts transformed data from staging tables into the final fact and dimension tables.
    This function processes the staged data and loads it into the star schema tables.
    """
    
    print("\n --- Inserting Data Into Tables --- \n")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def analyze_tables(cur, conn):
    
    """
    Performs basic data quality checks by printing row counts for all tables.
    This function executes analysis queries to verify data loading success.
    
    """
    print("\n --- Analyzing Tables --- \n")
    for query in analyze_queries:
        cur.execute(query)
        result = cur.fetchone()
        if result:
            table_name = query.replace('SELECT COUNT(*) AS total FROM ','').strip().replace(';','').replace('"','')
            print(f"SELECT COUNT(*) FROM {table_name}\n [{result[0]}]")
        else:
            print(f"Error analyzing table: {query} throwing error")

def main():
    
    """
    Connects to the Redshift database, runs ETL processes, and performs analysis.
    This is the main function that orchestrates the data loading pipeline.
    
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Successfully Connected To Redshift. \n")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    analyze_tables(cur,conn)

    conn.close()
    print("\n Connection closed. ETL process completed. \n")


if __name__ == "__main__":
    main()