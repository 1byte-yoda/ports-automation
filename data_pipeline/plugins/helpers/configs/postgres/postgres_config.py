# helpers/lib/postgres/postgres_config.py


class PostgresConfig:
    def __init__(self, conn_id: str, table: str = '', db: str = ''):
        self.conn_id = conn_id
        self.db = db
        self.table = table
