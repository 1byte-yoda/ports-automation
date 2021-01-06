# helpers/lib/mongo/mongo_config.py


class MongoConfig:
    def __init__(self, conn_id: str, collection: str, db: str = ''):
        self.conn_id = conn_id
        self.db = db
        self.collection = collection
