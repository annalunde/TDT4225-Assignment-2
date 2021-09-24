from DbConnector import DbConnector
from decouple import config
from tabulate import tabulate
import datetime
import numpy as np
import pandas as pd
import os


class Program:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(255) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(255),
                   transportation_mode VARCHAR(255),
                   start_date_time DATETIME,
                   end_date_time DATETIME,
                   FOREIGN KEY (user_id)
                    REFERENCES User(id)
                    ON DELETE CASCADE )
                """

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME,
                   FOREIGN KEY (activity_id)
                    REFERENCES activity(id)
                    ON DELETE CASCADE 
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_user_data(self, table_name):
        for root, dirs, files in os.walk(config("FILEPATH")):
            with open(config("FILEPATH_LABELED_IDS"), "r") as labeled_ids:
                labeled_ids = [ids.strip() for ids in labeled_ids]

                for user_id in dirs:
                    has_labels = user_id in labeled_ids
                    query = """ INSERT INTO %s (id, has_labels) VALUES ('%s', %s) """
                    self.cursor.execute(
                        query % (table_name, user_id, has_labels))
                self.db_connection.commit()
                break
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc

    def insert_activity_data(self, table_name):
        with open(config("FILEPATH_LABELED_IDS"), "r") as labeled_ids:
            labeled_ids = [ids.strip() for ids in labeled_ids]

        for _, dirs, _ in os.walk(config("FILEPATH")):
            for user_id in dirs:
                filepath = config("FILEPATH") + "/" + user_id + "/Trajectory"
                i = 0
                for root, direct, files in os.walk(filepath):
                    for f in files:

                        df = pd.read_csv(filepath + "/" + f,
                                         delimiter="\n", skiprows=6, header=None)
                        if df.shape[0] > 2500:
                            continue
                        df2 = df.iloc[[0, -1]]
                        print(df2[0])
                        # start_time =
                        d = df2[0][0].split(",")
                        r = Datetime("".join(d[-2:]))
                        print(r, type(r))

                        #end_time = df2[0][1].split(",")
                    i += 1
                    if i == 1:
                        break
                break
            break

            """
            

            for user_id in dirs:
                has_labels = user_id in labeled_ids
                query =  INSERT INTO %s (id, has_labels) VALUES ('%s', %s) 
                self.cursor.execute(
                    query % (table_name, user_id, has_labels))
            self.db_connection.commit()
            break
            """
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc

            """
                   user_id VARCHAR(255),
                   transportation_mode VARCHAR(255),
                   start_date_time DATETIME,
                   end_date_time DATETIME,
            """

            # check that the plt files do not exceed 2500 trackpoints (lines) excluding headers
            # transportation mode must match exactly both start and end time for activity
            # Start_time and end_time for activities can be found by looking at the date and time for the first and last trackpoint in each .plt-file
            # Remember to keep track of activity-ids when inserting trackpoints, so that each trackpoint has the correct foreign key to the Activity table
            # When extracting transportation_mode from the labels.txt files, remember that you only have to consider the user-ids found in the labeled_ids.txt, as they are the only users where transportation_mode may not be null.

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = Program()

        program.create_user_table(table_name="User")
        # program.insert_user_data(table_name="User")
        # program.fetch_data(table_name="User")

        program.create_activity_table(table_name="Activity")
        program.insert_activity_data(table_name="Activity")

        # program.create_trackpoint_table(table_name="TrackPoint")

        # _ = program.fetch_data(table_name="Person")
        # program.drop_table(table_name="Person")
        # Check that the table is dropped
        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
