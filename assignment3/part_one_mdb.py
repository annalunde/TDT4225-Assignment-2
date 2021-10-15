from pprint import pprint
from DbConnector_MongoDB import DbConnector_MongoDB
from decouple import config
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
        self.connection = DbConnector_MongoDB()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collections = self.client['test'].list_collection_names()
        if collection_name not in collections:
            collection = self.db.create_collection(collection_name)
            print('Created collection: ', collection)

        """
        User collection:

        User = { 
        _id: ‘00-181’ (string) 
        has_labels: True/False (boolean) 
        } 
        """

    def insert_user_documents(self, collection_name):
        collection = self.db[collection_name]

        for root, dirs, files in os.walk(config("FILEPATH")):
            with open(config("FILEPATH_LABELED_IDS"), "r") as labeled_ids:
                labeled_ids = [ids.strip() for ids in labeled_ids]

                for user_id in dirs:
                    has_labels = user_id in labeled_ids
                    element = {"has_labels": has_labels}
                    # NOTE can use collection.insert_many(element)
                    collection.insert_one(element)
                break

    def insert_activity_documents(self, collection_name):
        collection = self.db[collection_name]

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
                    element = {"_id": counter, "transportation_mode": transportation_mode,
                               "start_date_time": start_time, "end_date_time": end_time, "user_id": user_id, }
                    collection.insert_one(element)
        json.dump(activity_ids, open(config("FILEPATH_ACTIVITY_IDS"), "w"))

    def insert_trackpoint_data(self, collection_name):
        collection = self.db[collection_name]
        counter = 1
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
                    element = {"_id": counter, "activity_id": activity_id, "lat": lat, "lon": lon,
                               "altitude": altitude, "date_days": date_days, "date_time": date_time, }
                    data.append(element)
                    counter += 1

                # inserts up to 2500 documents at a time, to increase efficiency
                collection.insert_many(data)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({"_id": 10000})
        for doc in documents:
            pprint(doc)
        print(collection.count())

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = Program()

        # program.create_coll(collection_name="User")
        # program.show_coll()
        # program.insert_user_documents(collection_name="User")
        #program.fetch_documents(collection_name="User")

        # program.create_coll(collection_name="Activity")
        # program.show_coll()
        # program.insert_activity_documents(collection_name="Activity")
        #program.fetch_documents(collection_name="Activity")

        #program.drop_coll(collection_name="TrackPoint")

        #program.create_coll(collection_name="TrackPoint")
        #program.show_coll()
        #program.insert_trackpoint_data(collection_name="TrackPoint")

        program.fetch_documents(collection_name="TrackPoint")

        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
