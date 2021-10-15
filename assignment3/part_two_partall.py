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
                    "difference": {
                        "$divide": [{"$subtract": ["$start_date_time", "$end_date_time"]}, 86400]
                        }}
            },
            {
                "$match": {"$and": [{"difference": {"$lt": -1}}, {"difference": {"$gt": -2}}]}
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
                    "activity_id": "$activity_id",
                    "lat": "$lat",
                    "lon": "$lon",
                    "year": {"$dateToString": {"format": "%Y","date": "$date_time"}}
                }
            },
            {
                "$match": {
                    "year": {"$eq": "2008"}
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
        executor.query_two(collection_name="Activity")
        executor.query_four(collection_name="Activity")
        executor.query_eight(collection_name="Activity")
        executor.query_ten(collection_activity="Activity", collection_trackpoint="TrackPoint")




    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == '__main__':
    main()
