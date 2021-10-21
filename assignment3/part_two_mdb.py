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

    def query_two(self, collection_name):
        """
        Find the average, minimum and maximum number of activities per user.
        """
        min_max_avg = self.db[collection_name].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "Maximum": {"$max": "$count"},
                    "Minimum": {"$min": "$count"},
                    "Average": {"$avg": "$count"}
                }
            }
        ])
        for a in min_max_avg:
            pprint(a)

        return min_max_avg

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

    def query_four(self, collection_name):
        """
        Find the number of users that have started the activity in one day and ended the activity the next day.
        NOTE : We assuming counting number of distinct users
        """

        users = self.db[collection_name].aggregate([
            {
                "$project": {
                    "user_id": "$user_id",
                    "start_date": {"$dateToString": {"format": '%Y-%m-%d', "date": "$start_date_time"}},
                    "end_date": {"$dateToString": {"format": '%Y-%m-%d', "date": "$end_date_time"}},
                }
            },
            {
                "$project": {
                    "user_id": "$user_id",
                    "start": {"$dateFromString": {"dateString": "$start_date"}},
                    "end": {"$dateFromString": {"dateString": "$end_date"}},
                }
            },
            {
                "$project": {
                    "user_id": "$user_id",
                    "difference": {"$divide": [{"$subtract": ["$start", "$end"]}, 24 * 60 * 60 * 1000]}
                }
            },
            {
                "$match": {"difference": {"$eq": -1}}
            },
            {
                "$group": {
                    "_id": "$user_id",
                    "NumActivities": {"$sum": 1}
                }
            }
        ])

        for u in users:
            pprint(u)

        return users

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

    def query_six(self, collection_trackpoint):
        """
        An infected person has been at position (lat, lon) (39.97548, 116.33031) at
        time ‘2008-08-24 15:38:00’. Find the user_id(s) which have been close to this
        person in time and space (pandemic tracking). Close is defined as the same
        minute (60 seconds) and space (100 meters).
        """
        users_close_time = self.db[collection_trackpoint].aggregate([
            {
                "$project": {
                    "activity_id": "$activity_id",
                    "lat": "$lat",
                    "lon": "$lon",
                    "date_time": "$date_time",
                    "date_covid": {"$dateFromString": {"dateString": "2008-08-24 15:38:00"}},
                    "time_difference": {"$abs": {"$divide": [{"$subtract": ["$date_covid", "$date_time"]}, 1000]}}
                }
            },
            {
                "$match": {"time_difference": {"$lt": 60}}
            },
            {
                "$project": {
                    "user_id": "$user_id",
                    "lat": "$lat",
                    "lon": "$lon"
                }
            }
        ])

        potential_users = list(users_close_time)
        location_infected = (39.97548, 116.33031)
        users = []

        for i in range(len(potential_users)):
            location_potential = (
                potential_users[i]['lat'], potential_users[i]['lon'])
            distance = haversine(location_potential,
                                 location_infected, unit="km")
            user_id = potential_users[i]['user_id']
            if distance < 0.1:
                if user_id not in users:
                    users.append(user_id)

        print(users)

        return users

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

    def query_eight(self, collection_name):
        """
        Find all types of transportation modes and count how many distinct users that
        have used the different transportation modes. Do not count the rows where the
        transportation mode is null.
        """

        transportation_modes = self.db[collection_name].aggregate([
            {
                "$match": {"transportation_mode": {"$ne": None}}
            },
            {
                "$group": {
                    "_id": {
                        "transportation_mode": "$transportation_mode",
                        "user_id": "$user_id"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.transportation_mode",
                    "numDistinctUsers": {"$sum": 1}
                }
            }

        ])

        for mode in transportation_modes:
            pprint(mode)

        return transportation_modes

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

    def query_ten(self, collection_activity, collection_trackpoint):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """
        activities = self.db[collection_activity].aggregate([
            {
                "$match": {"user_id": {"$eq": "112"}}
            },
            {
                "$match": {"transportation_mode": {"$eq": "walk"}}
            },
            {
                "$lookup": {
                    "from": collection_trackpoint,
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "joined_table"
                }
            },
            {
                "$unwind": "$joined_table"
            },
            {
                "$project": {
                    "activity_id": "$joined_table.activity_id",
                    "date_time": "$joined_table.date_time",
                    "date_year": {"$dateToString": {"format": '%Y', "date": "$joined_table.date_time"}},
                    "lat": "$joined_table.lat",
                    "lon": "$joined_table.lon"
                }
            },
            {
                "$match": {"date_year": {"$eq": '2008'}}
            },
            {
                "$sort": {"date_time": -1}
            }
        ])

        activities_list = list(activities)

        activity_dict = dict()
        for i in range(len(activities_list)):
            if activities_list[i]['activity_id'] in activity_dict:
                activity_dict[activities_list[i]['activity_id']].append(
                    (activities_list[i]['lat'], activities_list[i]['lon']))
            else:
                activity_dict[activities_list[i]['activity_id']] = [
                    (activities_list[i]['lat'], activities_list[i]['lon'])]

        distance = 0
        for value in activity_dict.values():
            for i in range(len(value) - 1):
                distance += haversine(value[i], value[i + 1], unit="km")

        print(distance)

        return distance

    def query_eleven(self, collection_activity, collection_trackpoint):
        user_altitudes_dict = dict()

        for activity_id in range(274, 275):
            print(activity_id)

            activity_altitudes = self.db[collection_trackpoint].aggregate([
                {
                    "$match": {"activity_id": {"$eq": activity_id}}
                },
                {
                    "$match": {"altitude": {"$ne": -777}}
                },
                {
                    "$group": {
                        "_id": "$activity_id",
                        "altitude_array": {"$push": "$altitude"}
                    }
                },
                {
                    "$addFields": {
                        "result": {
                            "$reduce": {
                                "input": "$altitude_array",
                                "initialValue": {
                                    "prevValue": -1,
                                    "calculatedValues": 0
                                },
                                "in": {
                                    "$cond": {
                                        "if": {
                                            "$eq": ["$$value.prevValue", -1]
                                        },
                                        "then": {
                                            "prevValue": "$$this",
                                            "calculatedValues": 0
                                        },
                                        "else": {
                                            "$let": {
                                                "vars": {
                                                    "newValue": {
                                                        "$subtract": ["$$this", "$$value.prevValue"]
                                                    }
                                                },
                                                "in": {
                                                    "$cond": {
                                                        "if": {
                                                            "$gt": ["$$newValue", 0]
                                                        },
                                                        "then": {
                                                            "prevValue": "$$this",
                                                            "calculatedValues": {
                                                                "$sum": [
                                                                    "$$value.calculatedValues", "$$newValue"
                                                                ]
                                                            }
                                                        },
                                                        "else": {
                                                            "prevValue": "$$this",
                                                            "calculatedValues": "$$value.calculatedValues"
                                                        }
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
                    "$project": {
                        "altitudeGained": "$result.calculatedValues"
                    }
                },
                {
                    "$lookup": {
                        "from": collection_activity,
                        "localField": "_id",
                        "foreignField": "_id",
                        "as": "joined_table"
                    }
                },
                {
                    "$unwind": "$joined_table"
                },
                {
                    "$project": {
                        "user_id": "$joined_table.user_id",
                        "metersGained": {"$multiply": ["$altitudeGained", 0.3048]}
                    }
                }
            ])

            activity_altitudes_list = list(activity_altitudes)

            for i in range(len(activity_altitudes_list)):
                if activity_altitudes_list[i]['user_id'] in user_altitudes_dict:
                    user_altitudes_dict[activity_altitudes_list[i]['user_id']
                                        ] += activity_altitudes_list[i]['metersGained']
                else:
                    user_altitudes_dict[activity_altitudes_list[i]['user_id']
                                        ] = activity_altitudes_list[i]['metersGained']

        for key, value in user_altitudes_dict.items():
            print("user_id: "+key+"\t"+"metersGained: "+str(value))

    def query_twelve(self, collection_name_trackpoints):
        """
        Find all users who have invalid activities, and the number of invalid activities per user
         - An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
        """
        for user in range(0, 182):
            user = str(user) if user >= 100 else "0" + str(user)
            user = user if int(user) >= 10 else "0" + user
            print(user)
            invalid_activities = self.db[collection_name_trackpoints].aggregate([
                {"$match": {"user_id": {"$eq": user}}},
                {
                    "$project": {
                        "user_id": "$user_id",
                        "activity_id": "$activity_id",
                        "date_time": "$date_time",
                        "tid": "$_id"
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
                        "activity_id": "$activity_id",
                        # "tid": "$data._id"
                    }
                },
                {"$match": {"calculatedValues": {"$elemMatch": {"$gte": 5}}}},
                {"$group":  {"_id": "$user_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gte": 1}}}
            ], allowDiskUse=True)
            pprint.pprint(list(invalid_activities))

        return invalid_activities


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
        executor.query_two(collection_name="Activity")
        executor.query_three(collection_name_activities="Activity")
        executor.query_four(collection_name="Activity")
        executor.query_five(collection_name_activities="Activity")
        """
        executor.query_six(collection_trackpoint="TrackPoint")
        """
        executor.query_seven(collection_name_activities="Activity")
        executor.query_eight(collection_name="Activity")
        executor.query_nine_a(collection_name_activities="Activity")
        executor.query_nine_b(collection_name_activities="Activity")
        executor.query_ten(collection_activity="Activity",
                           collection_trackpoint="TrackPoint")
        executor.query_eleven(collection_activity="Activity",
                              collection_trackpoint="TrackPoint")

        executor.query_twelve(
            collection_name_trackpoints="TrackPoint"
        )
        """
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == "__main__":
    main()
