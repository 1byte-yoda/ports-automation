import os
import sys
from datetime import datetime
import dotenv
from scrapy.utils.project import get_project_settings
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import InvalidTopicError


class KafkaTopicFactory:
    name = 'unece_ports'

    def __init__(self):
        settings_file_path = 'unece_ports.settings'
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
        _settings = get_project_settings()
        _brokers = _settings.get('KAFKA_BROKERS')
        self.admin_client = KafkaAdminClient(bootstrap_servers=_brokers)

    @classmethod
    def generate_name(cls):
        current_timestamp = datetime.now().strftime('%Y_%m_%d_%T')
        current_timestamp = current_timestamp.replace(':', '_')
        name = KafkaTopicFactory.name
        return f"{name}_{current_timestamp}"

    def generate_topic(self):
        name = KafkaTopicFactory.generate_name()
        topic = NewTopic(name=name, num_partitions=1, replication_factor=3)
        try:
            self.admin_client.create_topics(
                new_topics=[topic],
                validate_only=False
            )
            self.update_env_topic_name(name)
        except InvalidTopicError as e:
            sys.stderr.write(e)
        return topic
    
    def update_env_topic_name(self, name):
        base_dir = os.path.abspath(
            os.path.dirname(os.path.dirname(__file__))
        )
        env_file = os.path.join(base_dir, '.env')
        dotenv.set_key(env_file, 'KAFKA_TOPIC', name)


if __name__ == '__main__':
    kafka_topic = KafkaTopicFactory()
    kafka_topic.generate_topic()
