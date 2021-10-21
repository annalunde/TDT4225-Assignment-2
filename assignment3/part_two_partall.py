from datetime import datetime
from pprint import pprint
from DbConnector_MongoDB import DbConnector_MongoDB
from haversine import haversine


class QueryExecutor:

    def __init__(self):
        self.connection = DbConnector_MongoDB()
        self.client = self.connection.client
        self.db = self.connection.db

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

    def query_six(self, collection_activity, collection_trackpoint):
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
                }
            },
            {
                "$project": {
                    "activity_id": "$activity_id",
                    "lat": "$lat",
                    "lon": "$lon",
                    "time_difference": {"$abs": {"$divide": [{"$subtract": ["$date_covid", "$date_time"]}, 1000]}}
                }
            },
            {
                "$match": {"time_difference": {"$lt": 60}}
            },
            {
                "$lookup": {
                    "from": collection_activity,
                    "localField": "activity_id",
                    "foreignField": "_id",
                    "as": "joined_table"
                }
            },
            {
                "$project": {
                    "user_id": "$joined_table.user_id",
                    "lat": "$lat",
                    "lon": "$lon"
                }
            }
        ])

        potential_users = list(users_close_time)
        location_infected = (39.97548, 116.33031)
        users = []

        for i in range(len(potential_users)):
            location_potential = (potential_users[i]['lat'], potential_users[i]['lon'])
            distance = haversine(location_potential, location_infected, unit="km")
            user_id = potential_users[i]['user_id']
            if distance < 0.1:
                if user_id not in users:
                    users.append(user_id)

        print(users)

        return users


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
                activity_dict[activities_list[i]['activity_id']].append((activities_list[i]['lat'], activities_list[i]['lon']))
            else:
                activity_dict[activities_list[i]['activity_id']] = [(activities_list[i]['lat'], activities_list[i]['lon'])]

        distance = 0
        for value in activity_dict.values():
            for i in range(len(value) - 1):
                distance += haversine(value[i], value[i + 1], unit="km")

        print(distance)

        return distance

    def query_eleven(self, collection_activity, collection_trackpoint):
        user_altitudes_dict = dict()

        for user in range(0,182):
            user = str(user) if user >= 100 else "0" + str(user)
            user = user if int(user) >= 10 else "0" + user
            print("user", user)

            activity_altitudes = self.db[collection_trackpoint].aggregate([
                {
                    "$match": {"user_id": {"$eq": user}}
                },
                {
                    "$project": {
                        "user_id": "$user_id",
                        "activity_id": "$activity_id",
                        "altitude": "$altitude",
                    }
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
                                    "prevValue": None,
                                    "calculatedValues": 0
                                },
                                "in": {
                                    "$cond": {
                                        "if": {
                                            "$eq": ["$$value.prevValue", None]
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
                    "$group": {
                        "_id": "$user_id",
                        "totalAltitudes": {"$sum": "$result.calculatedValues"}
                    }
                },
                {
                    "$project": {
                        "user_id": user,
                        "metersGained": {"$multiply": ["$totalAltitudes", 0.3048]}
                    }
                }
            ])

            activity_altitudes_list = list(activity_altitudes)
            pprint(activity_altitudes_list)


            for i in range(len(activity_altitudes_list)):
                if activity_altitudes_list[i]['user_id'] in user_altitudes_dict:
                    user_altitudes_dict[activity_altitudes_list[i]['user_id']] += activity_altitudes_list[i]['metersGained']
                else:
                    user_altitudes_dict[activity_altitudes_list[i]['user_id']] = activity_altitudes_list[i]['metersGained']

        sort = sorted(user_altitudes_dict.items(), key=lambda x: x[1], reverse=True)

        for i in range(0, 20):
            print("user_id: " + sort[i][0]+ "\t" + "metersGained: " + str(sort[i][1]))


def main():
    executor = None
    try:
        executor = QueryExecutor()

        print("Executing Queries: ")
        #executor.query_two(collection_name="Activity")
        #executor.query_four(collection_name="Activity")
        #executor.query_six(collection_activity="Activity", collection_trackpoint="TrackPoint")
        #executor.query_eight(collection_name="Activity")
        #executor.query_ten(collection_activity="Activity", collection_trackpoint="TrackPoint")
        executor.query_eleven(collection_activity="Activity", collection_trackpoint="TrackPoint")




    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == '__main__':
    main()
