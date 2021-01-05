# jsonifier/get_ports_json.py


import sys
import os
from typing import Iterable
from dotenv import load_dotenv
from jsonstreams import Stream, Type
import psycopg2
from psycopg2.extras import RealDictCursor


load_dotenv()


def query_ports(conn: callable, itersize=2500) -> Iterable:
    """Query all available ports data and store into a generator.
    @param conn: psycopg2 connect function to connect into the
    database.
    @param itersize: chunk size of iterable to avoid memory
    leaks.
    """
    try:
        cursor = conn.cursor('ports_cursor', cursor_factory=RealDictCursor)
        cursor.itersize = itersize
        query = """SELECT id, countryname AS "countryName",
        portname AS "portName", coordinates, unlocode FROM ports;"""
        cursor.execute(query)
        return cursor
    except psycopg2.OperationalError as e:
        sys.stderr.write(str(e.args))
    except Exception as e:
        sys.stderr.write(str(e.args))


def save_to_json(iterable: Iterable, filename: str):
    """Save an iterable into a json file through streaming
    to avoid memory leaks.
    @param iterable: can be list or generator that will be looped
    through when writting into a json file.
    @param filename: exact path + file name of the json file.
    """
    try:
        with Stream(Type.object, filename=filename) as s:
            with s.subarray('ports') as port:
                for row in iterable:
                    port.write(row)
    except FileNotFoundError as e:
        sys.stderr.write(str(e.args))
    except Exception as e:
        sys.stderr.write(str(e.args))


def get_save_file_path():
    base_dir = os.path.abspath(
        os.path.dirname(os.path.dirname(__file__))
    )
    file_name = 'ports_.json'
    file_dir = os.path.join(
        base_dir, 'jsonifier', 'output', file_name
    )
    return file_dir


def get_json_from_db():
    psql_url = os.environ.get('POSTGRESQL_URI')
    conn = psycopg2.connect(psql_url)
    ports_iter = query_ports(conn)
    file_dir = get_save_file_path()
    save_to_json(iterable=ports_iter, filename=file_dir)
    conn.close()
