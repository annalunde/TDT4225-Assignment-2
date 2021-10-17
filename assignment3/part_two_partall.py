from datetime import datetime
from pprint import pprint
from DbConnector_MongoDB import DbConnector_MongoDB


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
        documents = self.db[collection_trackpoint].aggregate([
            {
                "$project": {
                    "activity_id": "$activity_id",
                    "lat": "$lat",
                    "lon": "$lon",
                    "day_covid": {"$dateFromString": {"dateString": "2008-08-24 15:38:00"}},
                    "time_difference": {"$divide": [{"$subtract": ["day_covid", "$day_time"]}, 1000]}
                }
            }
        ])


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

        documents = self.db[collection_activity].aggregate([
            {
                "$match": {"user_id": {"$eq": '112'}}
            },
            {
                "$match": {"transportation_mode": {"$eq": 'walk'}}
            },
            {
                "$lookup": {
                    "from": collection_trackpoint,
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "Table"
                }
            },
            {
                "$project": {
                    "activity_id": "$Table.activity_id",
                    "lat": "$Table.lat",
                    "lon": "$Table.lon",
                    "start_year": {"$dateToString": {"format": "%Y","date": "$start_date_time"}},
                    "end_year": {"$dateToString": {"format": "%Y", "date": "$end_date_time"}}
                }
            },
            {
                "$match": {
                    "$and": [{"start_year": {"$eq": "2008"}},{"end_year": {"$eq": "2008"}}]
                }
            },
        ])

        for doc in documents:
            pprint(doc)




def main():
    executor = None
    try:
        executor = QueryExecutor()

        print("Executing Queries: ")
        #executor.query_two(collection_name="Activity")
        #executor.query_four(collection_name="Activity")
        executor.query_six(collection_activity="Activity", collection_trackpoint="TrackPoint")
        #executor.query_eight(collection_name="Activity")
        #executor.query_ten(collection_activity="Activity", collection_trackpoint="TrackPoint")




    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == '__main__':
    main()
