from pymongo import MongoClient
import configparser
from libs import Collector
from libs import FrameworkUi
from libs import DataAnalyzer

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')

    analyzer = DataAnalyzer(config['global']['mongodb'],
                            config['global']['raw_db'],
                            config['global']['result_db'])

    collector_thread = Collector(config['global']['mongodb'],
                                 config['global']['kafka'],
                                 config['global']['kafka_topic'],
                                 config['global']['raw_db'],
                                 analyzer)
    # collector_thread.start()
    # FrameworkUi.start()

    collection = 'test_1'
    mongo_conn = MongoClient(config['global']['mongodb'])
    db = mongo_conn[config['global']['result_db']]
    col = db[collection]