# data_pipeline/dags/unece_dag.py


import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from custom_operators.stage_operator import StageDatatoMongodbOperator
from custom_operators.data_quality_operator import DataQualityCheckOperator
from custom_operators.save_json_operator import LoadToJsonOperator
from custom_operators.transform_load_operator import TransformAndLoadOperator
from helpers.configs.postgres.postgres_config import PostgresConfig
from helpers.configs.mongo.mongo_config import MongoConfig
from helpers.lib.data_processors import PortsItemProcessor
from helpers.lib.sql_queries import SqlQueries


mongo_staging_config = MongoConfig('mongo_staging', 'ports')
postgres_master_config = PostgresConfig('postgres_master')


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'unece_ports@yopmail.com',
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=10)
}


dag = DAG(
    dag_id='unece_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


begin_execution = DummyOperator(
    task_id='begin_execution',
    dag=dag
)


check_ports_scraper_contracts = BashOperator(
    task_id="check_ports_scraper_contracts",
    bash_command="cd ~/airflow/scraper && scrapy check ports_spider",
    dag=dag
)


scrape_unece_data = BashOperator(
    task_id="scrape_unece_data",
    bash_command="cd ~/airflow && python scraper/main.py",
    dag=dag
)


stage_to_mongodb = StageDatatoMongodbOperator(
    task_id='stage_to_mongodb',
    dag=dag
)


transform_and_load_to_postgres = TransformAndLoadOperator(
    mongo_config=mongo_staging_config,
    postgres_config=postgres_master_config,
    task_id='transform_and_load_to_postgres',
    processor=PortsItemProcessor(),
    query=SqlQueries.ports_table_insert,
    query_params={"updated_at": datetime.datetime.utcnow()},
    dag=dag
)


check_data_quality = DataQualityCheckOperator(
    tables=['ports'],
    postgres_config=postgres_master_config,
    task_id='check_master_data_quality',
    queries={
        "ports_row_count": SqlQueries.ports_row_count,
        "ports_updated_count": SqlQueries.ports_updated_count
    },
    dag=dag
)


load_to_analytics_dw = DummyOperator(
    task_id='load_to_analytics_dw',
    dag=dag
)


check_dw_data_quality = DummyOperator(
    task_id='check_dw_data_quality',
    dag=dag
)


export_to_json = LoadToJsonOperator(
    task_id='export_to_json',
    postgres_config=postgres_master_config,
    query=SqlQueries.select_all_query_to_json,
    path='output/json',
    tables=['ports'],
    filenames={'ports': 'ports.json'},
    dag=dag
)


send_notification_email = EmailOperator(
    task_id='send_notification_email',
    to='unece_ports@yopmail.com',
    subject='Notification for unece_dag',
    html_content='<h3>unece_data_pipeline executed successfully</h3>',
    dag=dag
)


send_notification_slack = SlackAPIPostOperator(
    task_id='send_notification_slack',
    username='airflow',
    token=Variable.get('slack_api_key'),
    text='unece_dag executed successfully',
    channel='#automationcase',
    dag=dag
)


end_execution = DummyOperator(
    task_id='end_execution',
    dag=dag
)


begin_execution >> check_ports_scraper_contracts >> scrape_unece_data
scrape_unece_data >> stage_to_mongodb
stage_to_mongodb >> transform_and_load_to_postgres
transform_and_load_to_postgres >> check_data_quality
check_data_quality >> [export_to_json, load_to_analytics_dw]
load_to_analytics_dw >> check_dw_data_quality
check_dw_data_quality >> [send_notification_email, send_notification_slack]
export_to_json >> [send_notification_email, send_notification_slack]
send_notification_email >> end_execution
send_notification_slack >> end_execution
