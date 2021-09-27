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

    def query_twelve(self, table_name, limit):
        """
        Find all users who have invalid activities, and the number of invalid activities per user
            ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
        """
        query = "SELECT * FROM %s WHERE transportation_mode <> 'None' LIMIT %s"

        self.cursor.execute(query % (table_name, limit))
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    executor = None
    try:
        executor = QueryExecutor()

        executor.show_tables()

        _ = executor.query_twelve(table_name="Activity", limit=10)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == "__main__":
    main()
