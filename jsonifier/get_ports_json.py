import sys
from typing import Iterable
from jsonstreams import Stream, Type
import psycopg2
from psycopg2.extras import RealDictCursor


def query_ports(conn):
    cursor = conn.cursor('ports_cursor', cursor_factory=RealDictCursor)
    cursor.itersize = 2500
    query = 'SELECT * FROM ports;'
    cursor.execute(query)
    return cursor


def save_to_json(iterable: Iterable, filename: str):
    try:
        with Stream(Type.object, filename=filename) as s:
            with s.subarray('ports') as port:
                for row in iterable:
                    port.write(row)
    except Exception as e:
        sys.stderr.write(e.args[0])


if __name__ == '__main__':
    conn = psycopg2.connect(
        'postgresql://postgres:password@localhost/unece_dev'
    )
    ports_iter = query_ports(conn)
    filename = 'output/ports.json'
    save_to_json(iterable=ports_iter, filename=filename)
    conn.close()
