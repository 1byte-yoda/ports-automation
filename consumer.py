import os
import json
from kafka import KafkaConsumer
from kafka import TopicPartition
import dotenv


dotenv.load_dotenv()


topic = os.environ.get('KAFKA_TOPIC')
brokers = os.environ.get('KAFKA_BROKERS').split(',')
print(brokers)
consumer = KafkaConsumer(
    bootstrap_servers=brokers
)
tp = TopicPartition(topic, 0)
consumer.assign([tp])
consumer.seek_to_end(tp)
last_offset = consumer.position(tp)
consumer.seek_to_beginning(tp)


for message in consumer:
    print(json.loads(message.value))
    if message.offset == last_offset - 1:
        break

consumer.unsubscribe()
consumer.close()

# TODO: Export all messages to mongoDB

