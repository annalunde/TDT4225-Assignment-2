from DbConnector import DbConnector
from decouple import config
from tabulate import tabulate
import datetime
import numpy as np
import pandas as pd


class QueryExecutor:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query_one(self, table_name_users, table_name_activities, table_name_trackpoints):
        # How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        query = "SELECT UserCount.NumUsers, ActivitiesCount.NumActivities, TrackpointCount.NumTrackpoints FROM " \
            "(SELECT COUNT(*) as NumUsers FROM %s) AS UserCount," \
            "(SELECT COUNT(*) as NumActivities FROM %s) AS ActivitiesCount," \
            "(SELECT COUNT(*) as NumTrackpoints FROM %s) AS TrackpointCount"
        self.cursor.execute(
            query % (table_name_users, table_name_activities, table_name_trackpoints))
        rows = self.cursor.fetchall()
        print(rows)
        # Using tabulate to show the table in a nice way
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_two(self, table_name):
        # Find the average, minimum and maximum number of activities per user.
        query = "SELECT MAX(count) as Maximum," \
                "MIN(count) as Minimum," \
                "AVG(count) as Average " \
                "FROM (SELECT COUNT(*) as count FROM %s GROUP BY user_id) as c"

        self.cursor.execute(query % (table_name))
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_three(self, table_name_activities):
        # Find the top 10 users with the highest number of activities
        query = "SELECT user_id, COUNT(*) as Count " \
                "FROM %s " \
                "GROUP BY user_id " \
                "ORDER BY Count DESC " \
                "LIMIT 10"

        self.cursor.execute(query % table_name_activities)
        rows = self.cursor.fetchall()
        # print("Data from table %s, %s, raw format:" % table_name_users, table_name_activities)
        print(rows)
        # Using tabulate to show the table in a nice way
        # print("Data from table %s, %s, tabulated:" % table_name_users, table_name_activities)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_four(self, table_name):
        # Find the number of users that have started the activity in one day and ended the activity the next day.
        # TODO : Write Assuming counting number of distinct users
        # DATEDIFF returns the difference in days between two date values.
        query = "SELECT COUNT(DISTINCT user_id) as NumUsers " \
                "FROM %s " \
                "WHERE DATEDIFF(start_date_time, end_date_time) = -1"

        self.cursor.execute(query % (table_name))
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_five(self, table_name_activities):
        # Find activities that are registered multiple times. You should find the query
        # even if you get zero results.
        # TODO in report: discuss whether transportation_mode should be included here.
        query = "SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*) AS NumDuplicates " \
                "FROM %s " \
                "GROUP BY user_id, transportation_mode, start_date_time, end_date_time " \
                "HAVING NumDuplicates >1 " \

        self.cursor.execute(query % table_name_activities)
        rows = self.cursor.fetchall()
        # print("Data from table %s, %s, raw format:" % table_name_users, table_name_activities)
        print(rows)
        # Using tabulate to show the table in a nice way
        # print("Data from table %s, %s, tabulated:" % table_name_users, table_name_activities)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_six(self, table_name_activities, table_name_trackpoints):
        """
        Find the number of users which have been close to each other in time and
        space (Covid-19 tracking). Close is defined as the same minute (60 seconds)
        and space (100 meters).
        """

        query = "SELECT t1.user_id, t1.lat, t1.lon, t2.user_id, t2.lat, t2.lon " \
                "FROM (SELECT user_id, lat, lon, date_time FROM %s inner join %s on Activity.id=TrackPoint.activity_id) as t1, " \
                "(SELECT user_id, lat, lon, date_time FROM Activity inner join TrackPoint on Activity.id=TrackPoint.activity_id) as t2 " \
                "where t1.user_id != t2.user_id " \
                # "AND ABS(TIMESTAMPDIFF(SECOND,t1.date_time, t2.date_time)) <= 60" \

        self.cursor.execute(query % (table_name_activities,
                                     table_name_trackpoints))
        rows = self.cursor.fetchall()
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Results from query_six: ")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_seven(self, table_name_activities):
        # Find all users that have never taken a taxi.
        # TODO: we only consider labeled activities, but not all activities for that user have to be labeled to consider that user to never have taken a taxi
        query = "SELECT user_id " \
                "FROM %s " \
                "WHERE transportation_mode != 'taxi' AND transportation_mode <> 'None' " \
                "GROUP BY user_id "

        self.cursor.execute(query % table_name_activities)
        rows = self.cursor.fetchall()
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Results from query_seven_one_table: ")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_eight(self, table_name):
        # Find all types of transportation modes and count how many distinct users that
        # have used the different transportation modes. Do not count the rows where the
        # transportation mode is null.
        query = "SELECT transportation_mode as TransportationMode, COUNT(DISTINCT user_id) as NumDistinctUsers " \
                "FROM %s " \
                "WHERE transportation_mode <> 'None' " \
                "GROUP BY transportation_mode"

        self.cursor.execute(query % (table_name))
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_nine_a(self, table_name_activities):
        # a) Find the year and month with the most activities.
        # NOTE: I assume that if activities start in one month (year) and end the next month (year)
        # (e.g., start 30th december and end 1st january), they are counted regarding to the start_date_time
        query = "SELECT YEAR(start_date_time) as Year, MONTH(start_date_time) as Month, COUNT(*) AS ActivityCount  " \
                "FROM %s " \
                "GROUP BY YEAR(start_date_time), MONTH(start_date_time) " \
                "ORDER BY ActivityCount DESC " \
                "LIMIT 1 "

        self.cursor.execute(query % table_name_activities)
        rows = self.cursor.fetchall()
        # print("Data from table %s, %s, raw format:" % table_name_users, table_name_activities)
        print(rows)
        # Using tabulate to show the table in a nice way
        # print("Data from table %s, %s, tabulated:" % table_name_users, table_name_activities)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_nine_b(self, table_name_activities):

        # b) Which user had the most activities this year and month, and how many
        # recorded hours do they have? Do they have more hours recorded than the user
        # with the second most activities?

        # TODO:  Should the code be augmented to use variables? Current input for year and month is "hardkodet" from results of query_nine_a.
        query = "SELECT user_id, COUNT(*) AS ActivityCount" \
                ", SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as HoursActive " \
                "FROM %s " \
                "WHERE YEAR(start_date_time) = '2008' AND MONTH(start_date_time) = '11' " \
                "GROUP BY user_id " \
                "ORDER BY ActivityCount DESC " \
                "LIMIT 10" \

        self.cursor.execute(query % table_name_activities)
        rows = self.cursor.fetchall()
        # print("Data from table %s, %s, raw format:" % table_name_users, table_name_activities)
        print(rows)
        # Using tabulate to show the table in a nice way
        # print("Data from table %s, %s, tabulated:" % table_name_users, table_name_activities)
        print(tabulate(rows, headers=self.cursor.column_names))
        not_var = ""
        if rows[0][0][0] < rows[1][0][0]:
            not_var = "NOT"
        print("The user with the most activities does", not_var,
              "have more hours than the user with the second most activities.")
        return rows

    def query_ten(self, table_name_activities, table_name_trackpoints):
        # Find the total distance (in km) walked in 2008, by user with id=112.
        query = "SELECT Activity.id,lat,lon " \
                "FROM %s INNER JOIN %s on Activity.id = TrackPoint.activity_id " \
                "WHERE user_id='112' and " \
                "EXTRACT(YEAR FROM date_time) = 2008 " \
                "and transportation_mode='walk' " \
                "ORDER BY date_time ASC"

        self.cursor.execute(
            query % (table_name_activities, table_name_trackpoints))
        rows = self.cursor.fetchall()

        activity_dict = dict()
        for row in rows:
            if row[0] in activity_dict:
                activity_dict[row[0]].append((row[1], row[2]))
            else:
                activity_dict[row[0]] = [(row[1], row[2])]

        distance = 0
        distance_dict = dict()
        for key, value in activity_dict.items():
            distance_dict[key] = 0
            for i in range(len(value)-1):
                distance_dict[key] += haversine(value[i],
                                                value[i+1], unit='km')
                distance += haversine(value[i], value[i+1], unit='km')

        dict_distance = 0
        for value in distance_dict.values():
            dict_distance += value

        print(dict_distance)
        print(distance)

    def query_eleven(self, table_name_activities, table_name_trackpoints):
        # Find the top 20 users who have gained the most altitude meters
        # Denne koden gir samme svar som på Piazza mtp brukere, så burde være korrekt

        query = "SELECT user_id, SUM(AltitudeTPTable.altitudeGained)*0.3048 AS MetersGained " \
                "FROM %s INNER JOIN " \
                "   (SELECT id, activity_id, altitude, " \
                "   LAG(altitude) OVER (PARTITION BY activity_id) as PreviousAltitude, " \
                "   altitude - LAG(altitude) OVER(PARTITION BY activity_id) AS altitudeGained " \
                "   FROM %s " \
                "   WHERE altitude != -777 " \
                "   ) as AltitudeTPTable " \
                "ON Activity.id = AltitudeTPTable.activity_id " \
                "WHERE altitudeGained > 0 " \
                "GROUP BY user_id " \
                "ORDER BY MetersGained DESC " \
                "LIMIT 20"

        self.cursor.execute(
            query % (table_name_activities, table_name_trackpoints))
        rows = self.cursor.fetchall()
        # print("Data from table %s, %s, raw format:" % table_name_users, table_name_activities)
        print(rows)
        # Using tabulate to show the table in a nice way
        # print("Data from table %s, %s, tabulated:" % table_name_users, table_name_activities)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_twelve(self, table_name_activity, table_name_trackpoint):
        """
        Find all users who have invalid activities, and the number of invalid activities per user
            ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.
        """
        query = "WITH data as (SELECT user_id, date_time, TrackPoint.id as tid, activity_id, LEAD(date_time) OVER(PARTITION BY activity_id ORDER BY TrackPoint.id ASC) AS next_date_time, TIMESTAMPDIFF(MINUTE, date_time, LEAD(date_time) OVER(PARTITION BY activity_id ORDER BY TrackPoint.id ASC)) as difference FROM %s INNER JOIN %s on Activity.id = TrackPoint.activity_id ) " \
            "SELECT user_id, COUNT(DISTINCT activity_id) AS NumInvalid " \
            "FROM data " \
            "WHERE difference  >= 5 "\
            "GROUP BY user_id HAVING NumInvalid >= 1 "

        self.cursor.execute(
            query % (table_name_activity, table_name_trackpoint)
        )
        rows = self.cursor.fetchall()

        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    executor = None
    try:
        executor = QueryExecutor()

        executor.show_tables()

        """
        _ = executor.query_two(table_name="Activity")
        _ = executor.query_four(table_name="Activity")
        _ = executor.query_eight(table_name="Activity")
        _ = executor.query_ten(
            table_name_activities="Activity", table_name_trackpoints="TrackPoint")
        
        _ = executor.query_eleven(
            table_name_activities="Activity", table_name_trackpoints="TrackPoint")
        
        _ = executor.query_twelve(
            table_name_activity="Activity", table_name_trackpoint="TrackPoint")
        """
        _ = executor.query_six(
            table_name_activities="Activity", table_name_trackpoints="TrackPoint")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if executor:
            executor.connection.close_connection()


if __name__ == "__main__":
    main()
