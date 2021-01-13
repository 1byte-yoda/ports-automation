# data_pipeline/tests/conftest.py


import os
import datetime
import pytest
import mongomock
from airflow.models import DAG, DagBag
from pytest_postgresql.factories import postgresql_noproc, postgresql


pytest_plugins = ["helpers_namespace"]
COMMON_BASE_DIR = os.path.join(os.path.dirname(__file__), 'common')


@pytest.fixture
def dag(tmpdir):
    return DAG(
        "dag",
        default_args={
            "owner": "airflow",
            "start_date": datetime.datetime(2021, 1, 1),
        },
        template_searchpath=str(tmpdir),
        schedule_interval="@daily",
    )


@pytest.fixture(scope="session")
def dagbag():
    return DagBag()


@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={
            "owner": "test",
            "start_date": datetime.datetime(2020, 1, 1)
        },
        schedule_interval="@daily",
    )


@pytest.fixture(autouse=True)
def ports_collection():
    client = mongomock.MongoClient()
    collection = client['unece_test']['ports']
    collection.insert_one({
        'countryName': 'Philippines',
        'portName': 'Aleran/Ozamis',
        'unlocode': 'PH ALE',
        'coordinates': '4234N 00135E'
    })
    return collection


postgresql_proc = postgresql_noproc(
    host='postgresqldb_test',
    port=5432,
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD')
)


postgresql = postgresql(
    'postgresql_proc',
    load=[os.path.join(COMMON_BASE_DIR, 'db', 'postgres-init.sql')],
    db_name='unece_test'
)
