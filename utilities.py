# collection functions used across scripts

import configparser
import sqlite3
import os
import sys
import pymongo
import mongo_proxy
import logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(BASE_DIR))

def log_to_file(filename, level=logging.INFO):
    handler = logging.FileHandler (filename)
    handler.setLevel (level)
    formatter = logging.Formatter (fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter (formatter)
    loggername = filename
    logger = logging.getLogger (loggername)
    logger.setLevel (level)
    logger.addHandler (handler)

    return logger


def get_config():

    config = configparser.ConfigParser ()

    config_path = os.path.abspath (os.path.dirname (__file__))

    #print (euplexdb_tools_path)

    if "config.ini" in os.listdir(config_path):
        path = config_path + "/config.ini"
    else:
        raise Exception("config.ini not found!")

    config.read (path, encoding = 'utf-8')

    return config

def connect_mongodb(connect = True):
    config = get_config()

    mdb_client = mongo_proxy.MongoProxy(pymongo.MongoClient ("mongodb://" + config.get ('MONGODB', 'host') + ":" + config.get ('MONGODB', 'port') + "/", username = config.get('MONGODB', 'user'), password = config.get('MONGODB', 'password'), connect = connect))
    mdb_db = mdb_client[config.get ('MONGODB', 'db')]

    return mdb_db

def connect_sqlite(check_same_thread=False):

    config = get_config()

    db_path = os.path.join(BASE_DIR, config.get ('SQL', 'sqlite_db'))

    print(db_path)

    if os.path.exists(db_path):
        pass
    else:
        raise Exception("DB file does not exist!")


    #if config.getboolean('SQL', 'authentication'):
    #    psql_conn = sqlite3.connect (host=config.get ('PSQL', 'host'), database=config.get ('PSQL', 'db'),
    #                              user=config.get ('PSQL', 'user'), password=config.get ('PSQL', 'password'), port=config.get('PSQL', 'port'))
    #else:

    conn = sqlite3.connect (database=db_path, check_same_thread=check_same_thread)

    conn.isolation_level = None

    return conn

def add_mep_html_mongodb(html, mep_id, url, timestamp, replace, timediff, mdb_db):
    # @TODO: if timediff large than x between last saved html
    # @TODO: OR if change in HTML code and replace is False
    # @TODO: store in MongoDB

    pass
