import os
import mysql.connector as mysql
from dotenv import load_dotenv, find_dotenv
from rich import print
from tqdm import tqdm

load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the MySQL server, using credentials stored in
    environment variables.
    """

    def __init__(self, verbose=True):
        # Toggle whether or not debug print statements are used
        self.verbose = verbose

        # Connect to the MySQL server
        self.connection = mysql.connect(
            host=os.environ.get("HOST"),
            database=os.environ.get("MYSQL_DB"),
            user=os.environ.get("DB_USER_NAME"),
            password=os.environ.get("DB_PASSWORD"),
        )

        # Create a cursor
        self.cursor = self.connection.cursor()

        # Disable autocommit
        self.cursor.execute("SET autocommit=0;")

        # Check connection
        if self.verbose:
            db_version = self.connection.get_server_info()
            self.cursor.execute("select database();")
            database_name = self.cursor.fetchone()[0]
            print(f"Connected to MySQL version '{db_version}'")
            print(f"Database name: '{database_name}'")

    def execute_script(self, filename):
        # Open and read the file as a single buffer
        file = open(filename, "r", encoding="utf-8")

        # Read lines and strip line breaks
        lines = [x.strip().replace("\n", "") for x in file.read().split(";")]

        # Remove empty lines
        sql = [x for x in lines if x]
        file.close()

        # Execute every command from the input file
        for command in sql:
            try:
                self.cursor.execute(command)
            except Exception as e:
                print("Command skipped: ", command)
                print("Error:", e)

    def close(self):
        self.cursor.close()
        self.connection.close()
        if self.verbose:
            print("Connection to MySQL database closed")

    def make_st_db(self):
        self.execute_script("mysql_st_schema.sql")

    def reset_database(self):
        """
        Reset all tables used in the MySQL version of the experiment.
        """
        self.execute_script("mysql_schema.sql")



def insert_trans(db: Connector, table: str, data: list, batch_size: int, experiment_size: int):
    """
    Insert all supplied data into the specified table, in transactions of the
    given size.
    """
    query = f"""
    INSERT INTO {table} (tp_user, tp_point, tp_altitude, tp_date, tp_time)
    VALUES (%s, ST_GeomFromText('POINT(%s %s)'), %s, %s, %s)
    ;"""
    
    for i in tqdm(range(0, experiment_size, batch_size), leave=False):
        db.cursor.executemany(query, data[i : i + batch_size])
        db.connection.commit()
        
def insert(db: Connector, table: str, data: list, experiment_size: int):
    """
    Insert all supplied data into the specified table, in transactions of the
    given size.
    """
    query = f"""
    INSERT INTO {table} (tp_user, tp_point, tp_altitude, tp_date, tp_time)
    VALUES (%s, ST_GeomFromText('POINT(%s %s)'), %s, %s, %s)
    ;"""

    for i in tqdm(range(0, experiment_size)):
        db.cursor.execute(query, (data[i]))
        db.connection.commit()


def delete(db: Connector, table,experiment_size,preload_size=None):
    query = f"""
    DELETE 
    FROM {table} 
    WHERE tp_id = %s;"""
    for i in tqdm(range(1, experiment_size + 1)):
        indice = i if preload_size is None else  i + preload_size
        db.cursor.execute(query,(indice,)) 
        db.connection.commit()


def delete_trans(db: Connector, table, total_size,batch_size,preload_size=None):
    query = f"""DELETE 
    FROM {table} 
    WHERE tp_id = %s;"""
    for i in tqdm(range(1, total_size + 1,batch_size)):    
        for j in range(i, i + batch_size):
            indice = j if preload_size is None else  j + preload_size
            db.cursor.execute(query,(indice,))
        db.connection.commit()


def update(db:Connector,table: str, experiment_size,preload_size=None):
    query = f"""
    UPDATE {table} 
    SET tp_altitude = %s 
    WHERE tp_id = %s;"""

    for i in tqdm(range(1,experiment_size + 1)):
        indice = i if preload_size is None else i + preload_size
        db.cursor.execute(query,(1, indice,))
        db.connection.commit()


def update_trans(db: Connector, table : str, experiment_size,batch_size, preload_size=None):
    query = f"""
    UPDATE {table} 
    SET tp_altitude = %s 
    WHERE tp_id = %s;"""

    for i in tqdm(range(1, experiment_size + 1,batch_size)):    
        for j in range(i, i + batch_size):
            indice = j if preload_size is None else j + preload_size
            db.cursor.execute(query,(1,indice,))
        db.connection.commit()


