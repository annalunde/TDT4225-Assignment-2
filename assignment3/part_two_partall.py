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
        documents = self.db[collection_name].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "max": {"$max": "$count"},
                    "min": {"$min": "$count"},
                    "avg": {"$avg": "$count"}
                }
            }
        ])
        for doc in documents:
            pprint(doc)

    def query_four(self, collection_name):
        """
        Find the number of users that have started the activity in one day and ended the activity the next day.
        NOTE : We assuming counting number of distinct users
        """

        documents = self.db[collection_name].aggregate([
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

        for doc in documents:
            pprint(doc)

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


    def query_eight(self, collection_name):
        """
        Find all types of transportation modes and count how many distinct users that
        have used the different transportation modes. Do not count the rows where the
        transportation mode is null.
        """

        documents = self.db[collection_name].aggregate([
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

        for doc in documents:
            pprint(doc)


    def query_ten(self, collection_activity, collection_trackpoint):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """

        '''
        query = (
            "SELECT Activity.id,lat,lon "
            "FROM %s INNER JOIN %s on Activity.id = TrackPoint.activity_id "
            "WHERE user_id='112' and "
            "EXTRACT(YEAR FROM date_time) = 2008 "
            "and transportation_mode='walk' "
            "ORDER BY date_time ASC"
        )
        '''
        activities = self.db[collection_activity].aggregate([
            {
                "$match": {"user_id": {"$eq": "112"}}
            },
            {
                "$match": {"transportation_mode": {"$eq": "walk"}}
            },
            {
                "$match": {"transportation_mode": {"$ne": None}}
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




def main():
    executor = None
    try:
        executor = QueryExecutor()

        print("Executing Queries: ")
        #executor.query_two(collection_name="Activity")
        #executor.query_four(collection_name="Activity")
        #executor.query_six(collection_activity="Activity", collection_trackpoint="TrackPoint")
        #executor.query_eight(collection_name="Activity")
        executor.query_ten(collection_activity="Activity", collection_trackpoint="TrackPoint")




    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == '__main__':
    main()
