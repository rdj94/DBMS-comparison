import os
import psycopg2
from dotenv import load_dotenv, find_dotenv
from psycopg2.extras import execute_batch
from rich import print
from tqdm import tqdm

load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the Postgres server, using credentials stored in
    environment variables.
    """

    def __init__(self, verbose=True):
        # Toggle whether or not debug print statements are used
        self.verbose = verbose

        # Connect to the Postgres server
        self.connection = psycopg2.connect(
            host=os.environ.get("HOST"),
            database=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("DB_USER_NAME"),
            password=os.environ.get("DB_PASSWORD"),
        )

        # Create a cursor
        self.cursor = self.connection.cursor()

        # Check connection
        if self.verbose:
            self.cursor.execute("SELECT version()")
            db_version = self.cursor.fetchone()
            print(f"Connected to: {db_version[0]}")

    def close(self):
        self.cursor.close()
        self.connection.close()
        if self.verbose:
            print("Connection to Postgres database closed")

    def reset_database(self):
        """
        Reset all tables used in the Postgres version of the experiment.
        """
        with open("pg_schema.sql", "r") as file:
            self.cursor.execute(file.read())

    def make_st_db(self):
        with open("pg_st_schema.sql", "r") as file:
            self.cursor.execute(file.read())

def insert_trans(db: Connector, table: str, data: list, batch_size: int, experiment_size: int):
    """
    Insert all supplied data into the specified table, in transactions of the
    given size.
    """
    query = f"""
    --sql
    INSERT INTO {table} (tp_user, tp_point, tp_altitude, tp_date, tp_time)
    VALUES (%s, ST_MakePoint(%s,%s), %s, %s, %s)
    ;
    """

    for i in tqdm(range(0, experiment_size, batch_size), leave=False):
        execute_batch(db.cursor, query, data[i : i + batch_size])
    db.connection.commit()

def insert(db: Connector, table: str, data: list, experiment_size: int):
    """
    Insert all supplied data into the specified table, in transactions of the
    given size.
    """
    query = f"""
    --sql
    INSERT INTO {table} (tp_user, tp_point, tp_altitude, tp_date, tp_time)
    VALUES (%s, ST_MakePoint(%s,%s), %s, %s, %s)
    ;
    """

    for i in tqdm(range(0, experiment_size)):
        db.cursor.executemany(query, (data[i],))
        db.connection.commit()


def delete(db: Connector, table,experiment_size,preload_size=None):
    query = f"""
    --sql
    DELETE FROM {table}
    WHERE tp_id = %s;
    """
    for i in tqdm(range(1, experiment_size + 1)):
        indice = i if preload_size is None else  i + preload_size
        db.cursor.execute(query,(indice,)) 
        db.connection.commit()


def delete_trans(db: Connector, table, total_size,batch_size,preload_size=None):
    query = f"""
    --sql
    DELETE FROM {table}
    WHERE tp_id = %s;
    """
    for i in tqdm(range(1, total_size + 1,batch_size)):    
        for j in range(i, i + batch_size):
            indice = j if preload_size is None else  j + preload_size
            db.cursor.execute(query,(indice,))
        db.connection.commit()


def update(db:Connector,table: str, experiment_size,preload_size=None):
    query = f"""
    --sql
    UPDATE {table}
    SET tp_altitude = %s
    WHERE tp_id = %s;
    """

    for i in tqdm(range(1,experiment_size + 1)):
        indice = i if preload_size is None else i + preload_size
        db.cursor.execute(query,(1,indice))
        db.connection.commit()


def update_trans(db: Connector, table : str, experiment_size,batch_size, preload_size=None):
    query = f"""
    --sql
    UPDATE {table}
    SET tp_altitude = %s
    WHERE tp_id = %s;
    """

    for i in tqdm(range(1, experiment_size + 1,batch_size)):    
        for j in range(i, i + batch_size):
            indice = j if preload_size is None else j + preload_size
            db.cursor.execute(query,(1,indice))
        db.connection.commit()

