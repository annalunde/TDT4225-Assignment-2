from DbConnector_MongoDB import DbConnector_MongoDB
from decouple import config
from tabulate import tabulate
from haversine import haversine
import datetime
import numpy as np
import pandas as pd
import pprint


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

    def query_eleven():
        """
        Find the top 20 users who have gained the most altitude meters.
            1. Output should be a collection with (id, total meters gained per user).
            2. Remember that some altitude-values are invalid
            3. Tip: (tpn.altitude-tpn-1.altitude), tpn.altitude >tpn-1.altitude
        """

        """
        query = (
            "SELECT user_id, SUM(AltitudeTPTable.altitudeGained)*0.3048 AS MetersGained "
            "FROM %s INNER JOIN "
            "   (SELECT id, activity_id, altitude, "
            "   LAG(altitude) OVER (PARTITION BY activity_id) as PreviousAltitude, "
            "   altitude - LAG(altitude) OVER(PARTITION BY activity_id) AS altitudeGained "
            "   FROM %s "
            "   WHERE altitude != -777 "
            "   ) as AltitudeTPTable "
            "ON Activity.id = AltitudeTPTable.activity_id "
            "WHERE altitudeGained > 0 "
            "GROUP BY user_id "
            "ORDER BY MetersGained DESC "
            "LIMIT 20"
        )
        """

        top_twenty_users = self.db[collection_name_activities].aggregate([

            {"$group": {
                "_id": "$user_id",
                "count": {"$sum": 1}
            }
            },
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ])

        pprint.pprint(list(top_twenty_users))

        return top_twenty_users


def main():
    executor = None

    try:
        executor = QueryExecutor()

        print("Executing Queries: ")

        """
        _ = executor.query_one(
            collection_name_users="User",
            collection_name_activities="Activity",
            collection_name_trackpoints="TrackPoint",
        )


        _ = executor.query_three(collection_name_activities="Activity")

        _ = executor.query_five(collection_name_activities="Activity")

        _ = executor.query_seven(collection_name_activities="Activity")

        _ = executor.query_nine_a(collection_name_activities="Activity")

        """
        _ = executor.query_nine_b(collection_name_activities="Activity")

        """
        _ = executor.query_eleven(
            collection_name_activities="Activity", collection_name_trackpoints="TrackPoint"
        )
        """

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == "__main__":
    main()
