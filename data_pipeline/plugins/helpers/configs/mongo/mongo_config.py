# data_pipeline/plugins/helpers/config/mongo/mongo.py


class MongoConfig:
    """Class abstraction for setting up MongoDB configuration."""
    def __init__(self, conn_id: str, collection: str, db: str = ''):
        self.conn_id = conn_id
        self.db = db
        self.collection = collection
