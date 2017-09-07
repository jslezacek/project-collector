import threading
import json
from kafka import KafkaConsumer
from pymongo import MongoClient

TEST_ID_COLUMN = "testId";


class Collector(threading.Thread):
    def __init__(self, mongo_connection, kafka_connection, kafka_topic):
        threading.Thread.__init__(self)
        self.mongo_client = MongoClient(mongo_connection)
        self.kafka_consumer = KafkaConsumer(kafka_topic,
                                            bootstrap_servers=[kafka_connection],
                                            value_deserializer=lambda m: json.loads(m.decode('ascii')))

    def run(self):
        print("starting collector")
        measurement_header = None;
        db = self.mongo_client["test"]
        for message in self.kafka_consumer:
            decoded_msg = message.value;
            # check start of benchmark
            if TEST_ID_COLUMN in decoded_msg:
                measurement_header = decoded_msg[TEST_ID_COLUMN]
                print(measurement_header)
            # add benchmark id to measurements and store
            elif measurement_header:
                decoded_msg[TEST_ID_COLUMN] = measurement_header
                print(json.dumps(decoded_msg))
                result = db[measurement_header].insert_one(decoded_msg)
