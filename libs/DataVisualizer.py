from pymongo import MongoClient
import pandas as pd
from matplotlib.pyplot import plot


class DataVisualizer(object):
    benchmark_columns = {'lat1': ['x_cdf_lat1', 'x_cdf_lat2'],
                         'lat2': ['y_cdf_lat2', 'y_cdf_lat2']}

    def __init__(self, mongo_connection, results_db, test_list):
        self.mongo_conn = MongoClient(mongo_connection)
        self.tests = test_list
        self.results_db = results_db
        self.dataframe = pd.DataFrame()

    def plot(self):
        pass

    def _get_data(self):
        for test in self.tests:
            print(test)

