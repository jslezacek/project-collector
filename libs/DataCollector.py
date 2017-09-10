import threading
import json
from time import sleep
from enum import Enum
from kafka import KafkaConsumer
from pymongo import MongoClient


class Collector(threading.Thread):
    id_column = "testId"
    run_analysis = False

    def __init__(self, mongo_connection, kafka_connection,
                 kafka_topic, benchmark_db, data_analyzer):
        threading.Thread.__init__(self)
        self.database_raw = benchmark_db
        self.info = Enum('info', ['START', 'STOP'])
        self.test_name = None
        self.data_analyzer = None
        self.mongo_client = MongoClient(mongo_connection)
        self.data_analyzer = data_analyzer
        self.kafka_consumer = KafkaConsumer(kafka_topic,
                                            bootstrap_servers=[kafka_connection],
                                            value_deserializer=lambda m: json.loads(m.decode('ascii'))
                                            )

    def _set_test_name(self, testInfoMsg):
        self.test_name = testInfoMsg['testId']

    def run(self):
        print("starting collector")
        db = self.mongo_client[self.database_raw]
        for message in self.kafka_consumer:
            decoded_msg = message.value

            if "START" in decoded_msg:
                print("Received START")
                self._set_test_name(decoded_msg)
                db[self.test_name].drop()

            elif "STOP" in decoded_msg:
                # wait for all messages to arrive
                print("received STOP")
                self.data_analyzer.daemon = True
                self.data_analyzer.set_test_name(self.test_name)
                self.data_analyzer.start() # start analyser thread / delay 5s
                sleep(2)
                continue # skip and see if new messages arrived
            elif self.test_name:
                decoded_msg[self.id_column] = self.test_name
                print(json.dumps(decoded_msg))
                db[self.test_name].insert_one(decoded_msg)
