import dataclasses
import sqlite3
import subprocess
import pg
import mysql_ex as my
import mongo
import random
from rich import print
from dataclasses import dataclass, field
from time import perf_counter
from parser import mongo_parse, mysql_parse, pg_parse, read_data
from utils import time_this
from pymongo import MongoClient, GEOSPHERE

def make_st_mongo(conn,table_name,index):
    db = conn.db
    db.drop_collection(f"{table_name}")
    db.create_collection(f"{table_name}")
    if index:
        db[f"{table_name}"].create_index([("tp_point", GEOSPHERE)])


def make_st_my(db,table_name,index=False):

    drop_sql = f"""
    DROP TABLE IF EXISTS {table_name}
    """
    

    no_index_sql = f"""
    CREATE TABLE {table_name} (
    tp_id SERIAL PRIMARY KEY,
    tp_user INTEGER NOT NULL,
    tp_point GEOMETRY NOT NULL,
    tp_altitude FLOAT,
    tp_date VARCHAR(32),
    tp_time VARCHAR(32)
    );
    """
    index_sql = f"""
    CREATE TABLE {table_name} (
    tp_id SERIAL PRIMARY KEY,
    tp_user INTEGER NOT NULL,
    tp_point GEOMETRY NOT NULL,
    SPATIAL INDEX(tp_point),
    tp_altitude FLOAT,
    tp_date VARCHAR(32),
    tp_time VARCHAR(32)
    );
    """
    
    db.cursor.execute(drop_sql)
    db.cursor.execute(index_sql) if index else db.cursor.execute(no_index_sql)
    db.connection.commit()


def make_st_pg(db,table_name,index=False):
    drop_sql = f"""
    DROP TABLE IF EXISTS {table_name};
    """
    
    sql = f"""
    CREATE TABLE {table_name} (
    tp_id SERIAL PRIMARY KEY,
    tp_user INTEGER NOT NULL,
    tp_point GEOMETRY NOT NULL,
    tp_altitude FLOAT,
    tp_date VARCHAR(32),
    tp_time VARCHAR(32)
    );
    """
    db.cursor.execute(drop_sql)
    db.cursor.execute(sql)
    if index:
        create_index_sql= f"""CREATE INDEX {table_name}_geom_index ON {table_name} USING GIST (tp_point);"""
        db.cursor.execute(create_index_sql)

    db.connection.commit()

def get_dbms(dbms):
    db = None
    if dbms == 'postgres':
        db = pg.Connector(verbose=False)        
    elif dbms == 'mysql':
        db = my.Connector(verbose=False)
    elif dbms == 'mongodb':
        db = mongo.Connector(verbose=False)
    return db

if __name__== "__main__":
    raw_data = read_data()
    pg_data = pg_parse(raw_data)
    my_data = mysql_parse(raw_data)
    mongo_data = mongo_parse(raw_data)

    for dbms in ['mongodb']:
        conn = get_dbms(dbms)    
        for index in [True,False]:
            for datasize in [100000,500000,1000000,5000000,10000000]:
                table_name = f"st_trackpoint_{int(datasize / 1000)}k"
                table_name += '_indexed' if index else '_no_index'
                if dbms == 'postgres':
                    make_st_pg(conn,table_name, index)
                    pg.insert_trans(conn,table_name,pg_data,100000,datasize)
                if dbms == "mysql":
                    make_st_my(conn,table_name, index)
                    print(f'create {table_name} finished')
                    my.insert_trans(conn,table_name,my_data,100000,datasize)
                    print(f'data insert {datasize} finished')
                elif dbms == "mongodb":
                    make_st_mongo(conn,table_name,index)
                    print(f'create {table_name} finished')
                    mongo.insert_trans(conn,table_name,mongo_data,100000,datasize)
                    print(f'data insert {datasize} finished')
        conn.close()
    
    
    
 