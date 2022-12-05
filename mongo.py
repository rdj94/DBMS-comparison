import os
from dotenv import load_dotenv, find_dotenv
from rich import print
from pymongo import MongoClient, GEOSPHERE
from tqdm import tqdm


load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the MongoDB server, using credentials stored in
    environment variables.
    """

    def __init__(self, verbose=True):
        # Toggle whether or not debug print statements are used
        self.verbose = verbose

        # Connect to the Mongo server
        self.client = MongoClient(
            host=os.environ.get("HOST"),
            username=os.environ.get("DB_USER_NAME"),
            password=os.environ.get("DB_PASSWORD"),
        )

        self.db = self.client.get_database(name=os.environ.get("MONGO_DB"))

        # Check connection
        if self.verbose:
            server_info = self.client.server_info()
            print(f"Connected to: {server_info}")

    def close(self):
        self.client.close()
        if self.verbose:
            print("Connection to MongoDB database closed")


def make_st_base():
    conn = Connector(verbose=False)
    db = conn.db
    db.drop_collection("st_trackpoint_no_index")
    db.drop_collection("st_trackpoint_indexed")
    db.create_collection("st_trackpoint_no_index")
    db.create_collection("st_trackpoint_indexed")
    db["st_trackpoint_indexed"].create_index([("tp_point", GEOSPHERE)])
    
    conn.close()


def reset_database():
    """
    Reset all indexes and collection used in the MongoDB version of the experiment.
    """
    conn = Connector(verbose=False)
    db = conn.db
    db.drop_collection("trackpoint_no_index")
    db.drop_collection("trackpoint_indexed")
    db.create_collection("trackpoint_no_index")
    db.create_collection("trackpoint_indexed")
    db["trackpoint_indexed"].create_index([("tp_point", GEOSPHERE)])
    
    conn.close()


def insert_trans(
    conn: Connector,
    collection_name: str,
    records: list,
    batch_size: int,
    row_count: int,
):
    for i in tqdm(range(0, row_count, batch_size), leave=False):
        conn.db[collection_name].insert_many(records[i : i + batch_size])


def insert(conn: Connector,
    collection_name: str,
    records: list,
    row_count: int):
    for i in tqdm(range(0,row_count)):
        conn.db[collection_name].insert_one(records[i])


def delete(conn: Connector,
    collection_name: str,
    row_count: int,preload_size: int=None):
    for i in tqdm(range(0,row_count)):
        indice = i + 1 if preload_size is None else i + 1 + preload_size
        conn.db[collection_name].delete_one({'tp_id' : indice})

def delete_trans(
    conn: Connector,
    collection_name: str,
    row_count: int,
    batch_size: int,
    preload_size: int = None):
    for i in tqdm(range(0, row_count, batch_size), leave=False):
        indice =  i + 1 if preload_size is None else i + 1 + preload_size
        del_records = {'tp_id' : {"$in": [ j for j in range (indice, indice + batch_size)]}} 
        conn.db[collection_name].delete_many(del_records)

def update(conn: Connector,
    collection_name: str,
    row_count: int,preload_size: int=None):

    for i in tqdm(range(0, row_count), leave=False):
        indice =  i + 1 if preload_size is None else i + 1 + preload_size
        filter = {'tp_id': indice}
        new_value = { '$set': {'altitude' : 1}}
        conn.db[collection_name].update_one(filter,new_value)


def update_trans(conn: Connector,
    collection_name: str,
    row_count: int,batch_size:int,preload_size: int=None):

    for i in tqdm(range(0, row_count,batch_size), leave=False):
        indice =  i + 1 if preload_size is None else i + 1 + preload_size
        filter = {'tp_id' : {"$in": [ j for j in range (indice, indice + batch_size)]}} 
        new_value = { '$set': {'altitude' : 1}}
        conn.db[collection_name].update_many(filter,new_value)

def print_data(
    conn: Connector,table_name):
    posts = conn.db[table_name]
    import pprint
    for post in posts.find():
        pprint.pprint(post)