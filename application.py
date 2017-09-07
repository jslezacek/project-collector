from libs import Collector
from libs import FrameworkUi

MONGO_DB_NAME = "test"
KAFKA_TOPIC = "test"
MONGO_CONNECTION = "mongodb://framework:27017/test"
KAFKA_CONNECTION = "framework:9092"

if __name__ == "__main__":
    collector_thread = Collector(MONGO_CONNECTION,
                                 KAFKA_CONNECTION,
                                 KAFKA_TOPIC)
    collector_thread.start()
    # FrameworkUi.start()