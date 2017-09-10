import threading
from time import sleep
from pymongo import MongoClient
import pandas as pd
from tabulate import tabulate
import numpy as np
import sys


class DataAnalyzer(threading.Thread):
    map_columns = ['testId', 'feedId', 'orderId', 'orderTs']
    order_columns = ['orderId', 'orderTs']
    feed_columns = ['feedId', 'feedTs']
    rename_columns = {'feedTs_FeedPublisher': 'ts1',
                      'feedTs_FeedHandler': 'ts2',
                      'orderTs_OrderHandler': 'ts3',
                      'orderTs_OrderGateway': 'ts4'}

    result_columns = ['testId', 'feedId', 'orderId',
                      'ts1', 'ts2', 'ts3', 'ts4',
                      'lat1', 'lat2',
                      'x_cdf_lat1', 'y_cdf_lat1',
                      'x_cdf_lat2', 'y_cdf_lat2']
    collection = None

    def __init__(self, mongo_connection, raw_db, results_db):
        threading.Thread.__init__(self)
        self.raw_db = raw_db
        self.results_db = results_db
        self.mongo_conn = MongoClient(mongo_connection)
        self.dataframe = pd.DataFrame()

    def set_test_name(self, test_name):
        self.collection = test_name

    def run(self):
        print("Analyzer thread started.")
        sleep(5)
        self._get_full_data()
        self._merge_data()
        self._rename_columns()
        self._calculate_latencies()
        self._save_results()
        print(tabulate(self.dataframe[self.result_columns].head(10), headers='keys'))


    def _get_full_data(self):
        cursor = self.mongo_conn[self.raw_db][self.collection].find()
        self.dataframe = pd.DataFrame(list(cursor))
        print(tabulate(self.dataframe.head(10), headers='keys'))

    def _merge_data(self):
        # split per source
        df1 = self.dataframe[self.feed_columns].loc[self.dataframe['sourceId'] == 'FeedPublisher']
        df2 = self.dataframe[self.feed_columns].loc[self.dataframe['sourceId'] == 'FeedHandler']
        df3 = self.dataframe[self.order_columns].loc[self.dataframe['sourceId'] == 'OrderGateway']
        df_map = self.dataframe[self.map_columns].loc[self.dataframe['sourceId'] == 'OrderHandler']

        # merge and denormalize
        self.dataframe = pd.merge(df_map, df3, on='orderId', suffixes=('_OrderHandler', '_OrderGateway')) \
            .merge(df2, on='feedId') \
            .merge(df1, on='feedId', suffixes=('_FeedHandler', '_FeedPublisher'))

    def _rename_columns(self):
        self.dataframe = self.dataframe.rename(columns=self.rename_columns)

    def _calculate_latencies(self):
        # discrete array of samples
        self.dataframe = self.dataframe.apply(pd.to_numeric, errors='ignore')
        self.dataframe['lat1'] = self.dataframe['ts4'] - self.dataframe['ts1']
        self.dataframe['lat2'] = self.dataframe['ts3'] - self.dataframe['ts2']
        # calculate empirical cumulative distribution
        cdf_lat1 = self._calculate_cdf(self.dataframe['lat1'])
        cdf_lat2 = self._calculate_cdf(self.dataframe['lat2'])
        self.dataframe['x_cdf_lat1'] = cdf_lat1['x_cdf']
        self.dataframe['y_cdf_lat1'] = cdf_lat1['y_cdf']
        self.dataframe['x_cdf_lat2'] = cdf_lat2['x_cdf']
        self.dataframe['y_cdf_lat2'] = cdf_lat2['y_cdf']

    def _calculate_cdf(self, dataset):
        if dataset.size <= 1:
            print("Need more than 1 measurement to analyse")
            self._is_running = False
            sys.exit()
        sorted_dataset = np.sort(dataset)
        # generate an array for each data point. Divide it by the number of elements
        fractions = 1. * np.arange(sorted_dataset.size) / (sorted_dataset.size - 1)
        return {"x_cdf": sorted_dataset,
                "y_cdf": fractions}

    def _save_results(self):
        result_set = self.dataframe.to_dict('records')
        db = self.mongo_conn[self.results_db]
        db[self.collection].drop()
        db[self.collection].insert_many(result_set)
