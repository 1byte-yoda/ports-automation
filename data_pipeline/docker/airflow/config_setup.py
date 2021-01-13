# data_pipeline/docker/airflow/config_setup.py


from cryptography.fernet import Fernet
from os import environ
import configparser


def set_airflow_config(
    config_file, sqlalchemy_uri, executor, load_examples,
    smtp_user, smtp_password, smtp_port, smtp_mail_from,
    smtp_host
):
    """Set initial airflow config in airflow.cfg file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    # Setup config
    config['core']['sql_alchemy_conn'] = sqlalchemy_uri
    config['core']['executor'] = executor
    config['core']['load_examples'] = load_examples
    config['core']['fernet_key'] = Fernet.generate_key().decode()
    config['smtp']['smtp_user'] = smtp_user
    config['smtp']['smtp_password'] = smtp_password
    config['smtp']['smtp_host'] = smtp_host
    config['smtp']['smtp_port'] = smtp_port
    config['smtp']['smtp_mail_from'] = smtp_mail_from
    with open(config_file, 'w') as f:
        config.write(f)


if __name__ == '__main__':
    # Airflow Config
    host = environ.get('POSTGRES_HOST')
    user = environ.get('POSTGRES_USER')
    password = environ.get('POSTGRES_PASSWORD')
    port = environ.get('POSTGRES_PORT')
    airflow_db = environ.get('AIRFLOW_DB')
    airflow_postgres_conn_uri = (
        f'postgres://{user}:{password}@{host}:{port}/{airflow_db}'
    )
    executor = 'LocalExecutor'
    load_examples = 'False'
    smtp_user = environ.get('SMTP_USER')
    smtp_password = environ.get('SMTP_PASSWORD')
    smtp_mail_from = environ.get('SMTP_MAIL_FROM')
    smtp_port = environ.get('SMTP_PORT')
    smtp_host = environ.get('SMTP_HOST')

    # Set airflow configuration
    set_airflow_config(
        config_file='airflow.cfg',
        sqlalchemy_uri=airflow_postgres_conn_uri,
        executor=executor,
        load_examples=load_examples,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_port=smtp_port,
        smtp_mail_from=smtp_mail_from,
        smtp_host=smtp_host
    )
