from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable
from helpers import crawl_unece_ports
from configs import MongoConfig, PostgresConfig
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
    'email_on_failure': True,
    'email': 'unece_ports@yopmail.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


mongo_staging_config = MongoConfig('mongo_staging', 'ports')
postgres_master_config = PostgresConfig('postgres_master')


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
    mongo_config=mongo_staging_config,
    postgres_config=postgres_master_config,
    task_id='Load_to_postgres_master_db',
    dag=dag
)


run_data_quality_checks = DataQualityCheckOperator(
    tables=['ports'],
    postgres_config=postgres_master_config,
    task_id='Run_data_quality_checks',
    dag=dag
)


load_to_analytics_dw = DummyOperator(
    task_id='Load_to_analytics_dw',
    dag=dag
)


export_to_json = LoadToJsonOperator(
    task_id='Export_to_json',
    postgres_config=postgres_master_config,
    path='output/json',
    tables=['ports'],
    dag=dag
)


send_notification_email = EmailOperator(
    task_id='Send_notification_email',
    to='unece_ports@yopmail.com',
    subject='Notification for unece_data_pipeline',
    html_content='<h3>unece_data_pipeline executed successfully</h3>',
    dag=dag
)


send_notification_slack = SlackAPIPostOperator(
    task_id='Send_notification_slack',
    username='airflow',
    token=Variable.get('slack_api_key'),
    text='unece_data_pipeline executed successfully',
    channel='#automationcase',
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
[export_to_json, load_to_analytics_dw] >> send_notification_email
[export_to_json, load_to_analytics_dw] >> send_notification_slack
send_notification_email >> end_execution
send_notification_slack >> end_execution
