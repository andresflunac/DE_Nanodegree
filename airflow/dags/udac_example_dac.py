from datetime import datetime, timedelta
from pathlib import Path
from helpers import SqlQueries
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
    )

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 1, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    # Airflow UI connections
    'postgres_conn_id': 'redshift_connection',
    'aws_conn_id': 'aws_credentials',
    # Airflow UI variables
    's3_json_path': Variable.get("s3_json_path"),
    's3_bucket': Variable.get("s3_bucket"),
    's3_region': Variable.get("s3_region")
}

dag = DAG(
    dag_id='sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    template_searchpath=str(Path(__file__).parent.parent.joinpath("sql"))
    )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table="staging_events",
    s3_bucket=default_args['s3_bucket'],
    s3_key="log_data",
    s3_region=default_args['s3_region'],
    s3_json_path=default_args['s3_json_path']
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    target_table="staging_songs",
    s3_bucket=default_args['s3_bucket'],
    s3_key="song_data",
    s3_region=default_args['s3_region'],
    s3_json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table="songplays",
    fact_columns="playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
    sql=SqlQueries.songplay_table_insert,
    truncate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table="users",
    dimension_columns="userid, first_name, last_name, gender, level",
    sql=SqlQueries.user_table_insert,
    truncate=True
)
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table="songs",
    dimension_columns="songid, title, artistid, year, duration",
    sql=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table="artists",
    dimension_columns="artistid, name, location, lattitude, longitude",
    sql=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table="time",
    dimension_columns="start_time, hour, day, week, month, year, weekday",
    sql=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tests=[
        # Test 1: Null name values in artists table
        {
            'table': 'artists',
            'check_sql': 'SELECT SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) count_nulls',
            'expected_result': 0
        },
        # Test 2: Null name title in songs table
        {
            'table': 'songs',
            'check_sql': 'SELECT SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) count_nulls',
            'expected_result': 0
        },
        # Test 3: Null first_name values in users table
        {
            'table': 'users',
            'check_sql': 'SELECT SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) count_nulls',
            'expected_result': 0
        },
        # Test 4: Null userid values in songplays table
        {
            'table': 'songplays',
            'check_sql': 'SELECT SUM(CASE WHEN userid IS NULL THEN 1 ELSE 0 END) count_nulls',
            'expected_result': 0
        },
        # Test 5: Null year values in time table
        {
            'table': 'time',
            'check_sql': 'SELECT SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) count_nulls',
            'expected_result': 0
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Task dependencies definition
staging_tasks = [stage_events_to_redshift, stage_songs_to_redshift]
load_dim_tables = [
    load_song_dimension_table,
    load_artist_dimension_table,
    load_user_dimension_table,
    load_time_dimension_table
    ]
start_operator >> stage_create_tables >> staging_tasks >> load_songplays_table >> load_dim_tables >> run_quality_checks >> end_operator
