from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from helpers import DataQualityHelper, SqlQueries
from operators import (DataQualityOperator, LoadDimensionOperator, LoadFactOperator, StageToRedshiftOperator)

default_args: Dict = {
    'owner': 'sample-owner',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

with DAG(
        dag_id='etl_dag',
        default_args=default_args,
        description='Extract, load and transform data in Redshift with Airflow',
        schedule_interval='@hourly'
) as dag:
    redshift_conn_id: str = 'sample-data-pipeline-redshift-cluster'
    s3_conn_id: str = 'sample-data-pipeline'
    staging_event_source_path: str = 's3://udacity-dend/log_data/'
    staging_event_json_path_source_path: str = 's3://udacity-dend/log_json_path.json'
    staging_song_source_path: str = 's3://udacity-dend/song_data/'

    begin_execution_task = DummyOperator(task_id='begin_execution', dag=dag)
    end_execution_task = DummyOperator(task_id='end_execution', dag=dag)

    stage_event_to_redshift_task = StageToRedshiftOperator(
        task_id='stage_event_to_redshift',
        redshift_conn_id=redshift_conn_id,
        s3_conn_id=s3_conn_id,
        query_builder=SqlQueries.build_staging_events_copy_query_builder(
            s3_data_set_source_path=staging_event_source_path,
            s3_json_path_source_path=staging_event_json_path_source_path
        )
    )
    stage_song_to_redshift_task = StageToRedshiftOperator(
        task_id='stage_song_to_redshift',
        redshift_conn_id=redshift_conn_id,
        s3_conn_id=s3_conn_id,
        query_builder=SqlQueries.build_staging_songs_copy_query_builder(
            s3_data_set_source_path=staging_song_source_path
        )
    )

    load_songplay_fact_table_task = LoadFactOperator(
        task_id='load_fact_table',
        redshift_conn_id=redshift_conn_id,
        load_fact_table_query=SqlQueries.songplay_table_insert_query
    )

    load_user_dim_table_task = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_dim_table_query=SqlQueries.user_table_insert_query
    )
    load_artist_dim_table_task = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_dim_table_query=SqlQueries.artist_table_insert_query
    )
    load_time_dim_table_task = LoadDimensionOperator(
        task_id='load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_dim_table_query=SqlQueries.time_table_insert_query
    ),
    load_song_dim_table_task = LoadDimensionOperator(
        task_id='load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_dim_table_query=SqlQueries.song_table_insert_query
    )

    data_quality_task = DataQualityOperator(
        task_id='data_quality',
        redshift_conn_id=redshift_conn_id,
        data_quality_checker=DataQualityHelper.check_tables_for_non_empty_rows
    )

    begin_execution_task >> stage_event_to_redshift_task
    begin_execution_task >> stage_song_to_redshift_task

    stage_event_to_redshift_task >> load_songplay_fact_table_task
    stage_song_to_redshift_task >> load_songplay_fact_table_task

    load_songplay_fact_table_task >> load_user_dim_table_task
    load_songplay_fact_table_task >> load_artist_dim_table_task
    load_songplay_fact_table_task >> load_time_dim_table_task
    load_songplay_fact_table_task >> load_song_dim_table_task

    load_user_dim_table_task >> data_quality_task
    load_artist_dim_table_task >> data_quality_task
    load_time_dim_table_task >> data_quality_task
    load_song_dim_table_task >> data_quality_task

    data_quality_task >> end_execution_task
