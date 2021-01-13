# data_pipeline/docker/airflow/init_connections.py


from airflow import settings
from airflow.models import Connection
from os import environ


def create_connection(
    conn_id, conn_type, host, login, password, port, extra, schema
):
    """Create a connection in airflow's $database.connection table."""
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port,
        extra=extra,
        schema=schema
    )
    session = settings.Session()
    session.add(conn)
    session.commit()


if __name__ == '__main__':
    # Master DB setup
    master_db = environ.get('MASTER_DB')
    master_conn_id = environ.get('MASTER_CONNECTION_ID')
    master_conn_type = environ.get('MASTER_CONNECTION_TYPE')
    master_host = environ.get('POSTGRES_HOST')
    master_port = environ.get('POSTGRES_PORT')
    master_login = environ.get('POSTGRES_USER')
    master_password = environ.get('POSTGRES_PASSWORD')
    master_extra = environ.get('POSTGRES_EXTRAS')

    # Staging DB setup
    staging_db = environ.get('STAGING_DB')
    staging_conn_id = environ.get('STAGING_CONNECTION_ID')
    staging_conn_type = environ.get('STAGING_CONNECTION_TYPE')
    staging_host = environ.get('MONGO_HOST')
    staging_port = environ.get('MONGO_PORT')
    staging_login = environ.get('MONGO_INITDB_ROOT_USERNAME')
    staging_password = environ.get('MONGO_INITDB_ROOT_PASSWORD')
    staging_extra = environ.get('MONGO_EXTRAS')

    # Master DB Connection
    create_connection(
        conn_id=master_conn_id,
        conn_type=master_conn_type,
        host=master_host,
        login=master_login,
        password=master_password,
        port=master_port,
        extra=master_extra,
        schema=master_db
    )

    # Staging DB Connection
    create_connection(
        conn_id=staging_conn_id,
        conn_type=staging_conn_type,
        host=staging_host,
        login=staging_login,
        password=staging_password,
        port=staging_port,
        extra=staging_extra,
        schema=staging_db
    )
