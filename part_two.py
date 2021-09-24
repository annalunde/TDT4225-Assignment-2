from DbConnector import DbConnector
from decouple import config
from tabulate import tabulate
import datetime
import numpy as np
import pandas as pd


class QueryExecutor:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query_one(self, table_name):
        # How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        query = "SELECT * FROM  "

        self.cursor.execute(query % table_name)
        self.db_connection.commit()
