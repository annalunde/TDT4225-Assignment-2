from DbConnector_MongoDB import DbConnector_MongoDB
from decouple import config
from tabulate import tabulate
from haversine import haversine
import datetime
import numpy as np
import pandas as pd
import pprint
from tqdm import tqdm  # for progressbar on importing Trackpoint data


class QueryExecutor:
    def __init__(self):
        self.connection = DbConnector_MongoDB()
        self.client = self.connection.client
        self.db = self.connection.db

    def query_one(
        self, collection_name_users, collection_name_activities, collection_name_trackpoints
    ):
        """
        How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        """
        users = self.db[collection_name_users].aggregate([
            {"$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
            }
        ])
        activities = self.db[collection_name_activities].aggregate([
            {"$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
            }
        ])
        trackpoints = self.db[collection_name_trackpoints].aggregate([
            {"$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
            }
        ])

        pprint.pprint(list(users))
        pprint.pprint(list(activities))
        pprint.pprint(list(trackpoints))

        return users, activities, trackpoints

    def query_three(
        self, collection_name_activities
    ):
        """
        Find the top 10 users with the highest number of activities.
        """
        top_ten_users = self.db[collection_name_activities].aggregate([

            {"$group": {
                "_id": "$user_id",
                "count": {"$sum": 1}
            }
            },
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ])

        pprint.pprint(list(top_ten_users))

        return top_ten_users

    def query_five(
        self, collection_name_activities
    ):
        """
        Find activities that are registered multiple times. You should find the query
        even if you get zero results.
        """

        duplicates = self.db[collection_name_activities].aggregate([

            {"$group": {
                "_id": {
                    "user_id": "$user_id",
                    "transportation_mode": "$transportation_mode",
                    "start_date_time": "$start_date_time",
                    "end_date_time": "$end_date_time"},
                "count": {"$sum": 1}
            }
            },
            {"$match": {"count": {"$gt": 1}}}
        ])

        pprint.pprint(list(duplicates))

        return duplicates

    def query_seven(
        self, collection_name_activities
    ):
        """
        Find all users that have never taken a taxi.
        NOTE: We only consider labeled activities,
        but not all activities for that user have to be labeled to consider that user to never have taken a taxi
        """

        no_taxi = self.db[collection_name_activities].aggregate([

            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$match": {"transportation_mode": {"$ne": "taxi"}}},
            {"$group": {
                "_id": "$user_id"
            }
            }
        ])

        l = []
        for i in list(no_taxi):
            l.append(int(list(i.values())[0]))
        l.sort()
        print(l)

        return l

    def query_nine_a(
        self, collection_name_activities
    ):
        """
        Find the year and month with the most activities.
        NOTE: We assume that if activities start in one month (year) and end the next month (year)
        (e.g., start 30th december and end 1st january), they are counted regarding to the start_date_time
        """

        most_activities = self.db[collection_name_activities].aggregate([

            {"$group": {
                "_id": {
                    "year": {"$dateToString": {
                             "format": "%Y",
                             "date": "$start_date_time"
                             }},
                    "month": {"$dateToString": {
                        "format": "%m",
                        "date": "$start_date_time"
                    }}
                }, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1},
        ])

        pprint.pprint(list(most_activities))

        return most_activities

    def query_nine_b(self, collection_name_activities):
        """
        Which user had the most activities this year and month, and how many recorded hours do they have?
        Do they have more hours recorded than the user with the second most activities?
        """

        user_most_activities = self.db[collection_name_activities].aggregate([
            {
                "$project": {
                    "year": {"$dateToString": {"format": "%Y", "date": "$start_date_time"}},
                    "month": {"$dateToString": {"format": "%m", "date": "$start_date_time"}},
                    "user_id": "$user_id",
                    "end_date_time": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$end_date_time"}},
                    "start_date_time": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$start_date_time"}}
                }
            },
            {
                "$match": {
                    "year": {"$eq": "2008"},
                    "month": {"$eq": "11"}

                }
            },
            {
                "$project": {
                    "user_id": "$user_id",
                    "end_date_time": {"$dateFromString": {"dateString": "$end_date_time"}},
                    "start_date_time": {"$dateFromString": {"dateString": "$start_date_time"}}
                }
            },
            {
                "$project": {
                    "user_id": "$user_id",
                    "duration": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 60 * 60 * 1000]}
                }
            },
            {"$group": {
                "_id": "$user_id", "count": {"$sum": 1}, "total": {"$sum": "$duration"}}},
            {"$sort": {"count": -1}},
            {"$limit": 2}

        ])

        pprint.pprint(list(user_most_activities))

        return user_most_activities

    def query_twelve(self, collection_name_activities, collection_name_trackpoints):
        """
        Find all users who have invalid activities, and the number of invalid activities per user
         - An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
        """

        invalid_users = dict()

        for user in tqdm(range(0, 182), colour="#39ff14"):
            user = str(user) if user >= 100 else "0" + str(user)
            user = user if int(user) >= 10 else "0" + user
            invalid_activities = self.db[collection_name_activities].aggregate([
                {"$match": {"user_id": {"$eq": user}}},
                {
                    "$lookup": {
                        "from": collection_name_trackpoints,
                        "localField": "_id",
                        "foreignField": "activity_id",
                        "as": "data"
                    }
                },
                {"$unwind": "$data"},
                {
                    "$project": {
                        "user_id": "$data.user_id",
                        "activity_id": "$data.activity_id",
                        "date_time": "$data.date_time",
                        "tid": "$data._id"
                    }
                },
                {"$sort": {"tid": 1}},
                {"$group": {"_id": "$activity_id", "date_time_array": {"$push": "$date_time"}}
                 },
                {"$addFields": {
                    "result": {"$reduce": {
                        # walk array of $values with $reduce operator
                        "input": "$date_time_array",
                        "initialValue": {
                            "prevValue": None,
                            "calculatedValues": [],
                        },
                        "in": {"$cond": {
                            "if": {
                                # if we do not know two neighbouring values
                                # (first iteration)
                                "$eq": ["$$value.prevValue", None],
                            },
                            "then": {
                                # then we just skip the calculation
                                # for current iteration
                                "prevValue": '$$this',
                                "calculatedValues": []
                            },
                            "else": {
                                # otherwise we know two neighbouring values
                                # and it is possible to calculate the diff now
                                "$let": {
                                    "vars": {
                                        "newValue": {
                                            # calculate the diff
                                            "$divide": [{"$subtract": ["$$this", "$$value.prevValue"]}, 60 * 1000]
                                            # "$subtract": ['$$this', '$$value.prevValue'],
                                        }
                                    },
                                    "in": {
                                        "prevValue": "$$this",
                                        "calculatedValues": {
                                            # push the calculated value into array of results
                                            "$concatArrays": [
                                                "$$value.calculatedValues", [
                                                    "$$newValue"]
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                        }
                    }
                    }
                }
                },
                {
                    # restructure the output documents
                    "$project": {
                        # "initialValues": '$date_time_array',
                        "calculatedValues": '$result.calculatedValues',
                        "user_id": "$user_id",
                        "activity_id": "$data.activity_id",
                        # "tid": "$data._id"
                    }
                },
                {"$match": {"calculatedValues": {"$elemMatch": {"$gte": 5}}}},
                {"$group":  {"_id": "$user_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gte": 1}}}
            ])
            pprint.pprint(list(invalid_activities))

        # df = pd.DataFrame(list(invalid_activities))
        # result = pd.concat(all_df_real)
        # print(all_df_real)

        return invalid_activities


def main():
    executor = None

    try:
        executor = QueryExecutor()

        print("Executing Queries: ")

        """
        _=executor.query_one(
            collection_name_users="User",
            collection_name_activities="Activity",
            collection_name_trackpoints="TrackPoint",
        )

        _=executor.query_three(collection_name_activities="Activity")

        _=executor.query_five(collection_name_activities="Activity")

        _=executor.query_seven(collection_name_activities="Activity")

        _=executor.query_nine_a(collection_name_activities="Activity")

        _=executor.query_nine_b(collection_name_activities="Activity")

        """
        _ = executor.query_twelve(
            collection_name_activities="Activity", collection_name_trackpoints="TrackPoint"
        )

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == "__main__":
    main()
