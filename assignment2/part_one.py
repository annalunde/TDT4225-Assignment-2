from DbConnector_MySQL import DbConnector_MySQL
from decouple import config
from tabulate import tabulate
import datetime
import numpy as np
import pandas as pd
import json
import os
from os import listdir
from os.path import isfile, join
from tqdm import tqdm  # for progressbar on importing Trackpoint data


class Program:
    def __init__(self):
        self.connection = DbConnector_MySQL()
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
                    REFERENCES Activity(id)
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
        activity_ids = dict()
        with open(config("FILEPATH_LABELED_IDS"), "r") as labeled_ids:
            labeled_ids = [ids.strip() for ids in labeled_ids]

            dirs = [
                directory
                for directory in listdir(config("FILEPATH"))
                if not isfile(join(config("FILEPATH"), directory))
            ]

            # a counter used for keeping track of the activity id given in the database that matches the filename. Used later when inserting trackpoint data
            counter = 1

            for user_id in dirs:
                filepath = config("FILEPATH") + "/" + user_id + "/Trajectory"
                files = [f for f in listdir(
                    filepath) if isfile(join(filepath, f))]

                for f in files:

                    df = pd.read_csv(
                        filepath + "/" + f, delimiter="\n", skiprows=6, header=None
                    )

                    # does not insert activities where there are more than 2500 trackpoints
                    if df.shape[0] > 2500:
                        continue

                    key = user_id + "-" + f
                    activity_ids[key] = counter
                    counter += 1

                    df2 = df.iloc[[0, -1]]
                    d_start = df2.iloc[0, 0].split(",")
                    dt_start = d_start[-2] + " " + d_start[-1]
                    start_time = datetime.datetime.strptime(
                        dt_start, "%Y-%m-%d %H:%M:%S"
                    )
                    d_end = df2.iloc[-1, 0].split(",")
                    dt_end = d_end[-2] + " " + d_end[-1]
                    end_time = datetime.datetime.strptime(
                        dt_end, "%Y-%m-%d %H:%M:%S")

                    transportation_mode = None
                    if user_id in labeled_ids:
                        labels_filepath = (
                            config("FILEPATH") + "/" + user_id + "/labels.txt"
                        )
                        df_labels = pd.read_csv(
                            labels_filepath, delimiter="\t")

                        for _, row in df_labels.iterrows():
                            trckpt_s = row["Start Time"]
                            trckpt_e = row["End Time"]
                            start_time_trckpt = datetime.datetime.strptime(
                                trckpt_s, "%Y/%m/%d %H:%M:%S"
                            )
                            end_time_trckpt = datetime.datetime.strptime(
                                trckpt_e, "%Y/%m/%d %H:%M:%S"
                            )

                            # only include labeled data (transportation mode) where the start time and end time match
                            if (
                                start_time == start_time_trckpt
                                and end_time == end_time_trckpt
                            ):
                                transportation_mode = row["Transportation Mode"]

                    query = """ INSERT INTO %s (user_id, transportation_mode,start_date_time,end_date_time) VALUES ('%s', '%s', '%s', '%s')  """
                    self.cursor.execute(
                        query
                        % (
                            table_name,
                            user_id,
                            transportation_mode,
                            start_time,
                            end_time,
                        )
                    )
                    self.db_connection.commit()
        json.dump(activity_ids, open(config("FILEPATH_ACTIVITY_IDS"), "w"))

    def insert_trackpoint_data(self, table_name):
        dirs = [
            directory
            for directory in listdir(config("FILEPATH"))
            if not isfile(join(config("FILEPATH"), directory))
        ]

        activity_ids = json.load(
            open(config("FILEPATH_ACTIVITY_IDS"))
        )  # fetches the activity_ids dictionary created when importing the activity data

        for user_id in tqdm(dirs, colour="#39ff14"):  # progressbar on importing data
            filepath = config("FILEPATH") + "/" + user_id + "/Trajectory"
            files = [f for f in listdir(filepath) if isfile(join(filepath, f))]

            for f in files:
                df = pd.read_csv(
                    filepath + "/" + f, delimiter="\n", skiprows=6, header=None
                )

                # does not insert trackpoint data where there are more than 2500 trackpoints in one file
                if df.shape[0] > 2500:
                    continue

                # fetches the activity id from the activity_ids dictionary written to a file when inserting the activity data
                activity_id = int(activity_ids[user_id + "-" + f])

                data = []

                for trckpnt in df.values:
                    tp = np.array([x.split(",") for x in trckpnt])
                    lat = float(tp[0][0])
                    lon = float(tp[0][1])
                    altitude = float(tp[0][3])
                    date_days = float(tp[0][4])
                    date_time = datetime.datetime.strptime(
                        tp[0][-2] + " " + tp[0][-1], "%Y-%m-%d %H:%M:%S"
                    )
                    data.append(
                        (activity_id, lat, lon, altitude, date_days, date_time))

                query = """ INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s,%s)  """
                self.cursor.executemany(
                    query, data
                )  # inserts up to 2500 rows at a time, to increase efficiency
                self.db_connection.commit()

    def fetch_data(self, table_name, limit):
        query = "SELECT * FROM %s LIMIT %s"
        self.cursor.execute(query % (table_name, limit))
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

        program.show_tables()

        # User Table
        program.create_user_table(table_name="User")
        # program.insert_user_data(table_name="User")
        # program.fetch_data(table_name="User")

        # Activity Table
        program.create_activity_table(table_name="Activity")
        # program.insert_activity_data(table_name="Activity")
        # _ = program.fetch_data(table_name="Activity", limit=1000)

        # Trackpoint Table
        program.create_trackpoint_table(table_name="TrackPoint")
        # program.insert_trackpoint_data(table_name="TrackPoint")
        # program.fetch_data(table_name="TrackPoint", limit=100)

        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
