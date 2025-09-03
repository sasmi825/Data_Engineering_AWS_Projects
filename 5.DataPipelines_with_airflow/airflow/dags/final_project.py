from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('final_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_execution = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="staging_events",
    s3_bucket="sri-825-bucket",
    s3_key="log-data/{execution_date.year}/{execution_date.month}",
    json_path="s3://sri-825-bucket/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_name="staging_songs",
    s3_bucket="sri-825-bucket",
    s3_key="song-data",
    json_path="auto"
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dim_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="users",
    sql_query=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dim_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songs",
    sql_query=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dim_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="artists",
    sql_query=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dim_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="time",
    sql_query=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid IS NULL", 'expected_result': 0}
    ]
)

stop_execution = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define task dependencies
start_execution >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table
load_songplays_fact_table >> [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table]
[load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table] >> run_quality_checks
run_quality_checks >> stop_execution