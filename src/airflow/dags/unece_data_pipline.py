from __future__ import division, absolute_import, print_function
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import crawl_unece_ports
from operators import (
    WebScraperOperator,
    StageDatatoMongodb,
    DataQualityCheckOperator,
    LoadToMasterdbOperator,
    LoadToJsonOperator
)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'youremail@host.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='unece_data_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


begin_execution = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)


scrape_unece_ports_data = WebScraperOperator(
    task_id='Scrape_unece_ports_data',
    web_scraper_func=crawl_unece_ports,
    dag=dag
)


stage_data_to_mongodb = StageDatatoMongodb(
    task_id='Stage_to_mongodb',
    dag=dag
)


load_to_postgres_master_db = LoadToMasterdbOperator(
    mongo_conn_uri_key='stage_db_uri',
    mongo_collection='ports',
    mongo_db='unece_staging',
    postgres_conn_uri_key='master_db_uri',
    postgres_table='ports',
    task_id='Load_to_postgres_master_db',
    dag=dag
)


run_data_quality_checks = DataQualityCheckOperator(
    tables=['ports'],
    postgres_conn_uri_key='master_db_uri',
    task_id='Run_data_quality_checks',
    dag=dag
)


load_to_analytics_dw = DummyOperator(
    task_id='Load_to_analytics_dw',
    dag=dag
)


export_to_json = LoadToJsonOperator(
    task_id='Export_to_json',
    dag=dag
)


end_execution = DummyOperator(
    task_id='End_execution',
    dag=dag
)


begin_execution >> scrape_unece_ports_data
scrape_unece_ports_data >> stage_data_to_mongodb
stage_data_to_mongodb >> load_to_postgres_master_db
load_to_postgres_master_db >> run_data_quality_checks
run_data_quality_checks >> [export_to_json, load_to_analytics_dw]
[export_to_json, load_to_analytics_dw] >> end_execution
