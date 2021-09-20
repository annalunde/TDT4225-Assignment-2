from DbConnector import DbConnector
from tabulate import tabulate
import os


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   has_labels TINYINT)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id INT,
                   INDEX user_ind (user_id),
                   transportation_mode STRING,
                   start_date_time DATETIME,
                   end_date_time DATETIME,
                   FOREIGN KEY (user_id)
                    REFERENCES user(id)
                    ON DELETE CASCADE )
                """
        # DOUBLE CHECK: Should have cascade here?

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
                    REFERENCES activity(id)
                    ON DELETE CASCADE 
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_user_data(self, table_name, dataset_name):
        print("hei")
        for root, dirs, files in os.walk("/Users/Anna/Desktop/SDD/assignment2/dataset/dataset/Data", topdown=true):

            print(root)

            # print(dirs)

        with open("dataset/dataset/labeled_ids.txt", "r") as labeled_ids:
            for ids in labeled_ids:
                stripped_id = ids.strip()

            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            #query = "INSERT INTO %s (name) VALUES ('%s')"
            #self.cursor.execute(query % (table_name, name))
            # self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
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

        program.create_user_table(table_name="User")
        program.insert_user_data(
            table_name="User", dataset_name="dataset/dataset/Data")

        # program.create_activity_table(table_name="Activity")

        # program.create_trackpoint_table(table_name="TrackPoint")

        #_ = program.fetch_data(table_name="Person")
       # program.drop_table(table_name="Person")
        # Check that the table is dropped
        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
