import pymongo
import psycopg2


def stage_to_master():
    client = pymongo.MongoClient(
        "mongodb://root:rootpassword@mongodb:27017/tracking?authSource=admin",
        maxPoolSize=100
    )
    db = client['unece_staging']
    collection = db['ports']
    psql_conn = psycopg2.connect(
        'postgres://postgres:password@postgresqldb:5432/unece_production'
    )
    cursor = psql_conn.cursor()
    insert_statement = """INSERT INTO ports(
        countryName, portName, unlocode, coordinates, staging_id
    )
    VALUES (
        %(countryName)s,
        %(portName)s,
        %(unlocode)s,
        %(coordinates)s,
        %(staging_id)s
    );
    """
    for idx, item in enumerate(collection.find()):
        item['staging_id'] = item['_id'].__str__()
        item.pop('_id')
        cursor.execute(insert_statement, item)
    psql_conn.commit()
    psql_conn.close()
