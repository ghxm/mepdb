# collection functions used across scripts

import configparser
import sqlite3
import os
import sys
import pymongo
import mongo_proxy
import logging
import datetime

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

def add_mep_html_mongodb(html, mep_id, url = None, insert_only_update = True, timediff: datetime.timedelta = None, timestamp = None, replace = False, mdb_db = None):
    # @TODO: if timediff large than x between last saved html
    # @TODO: OR if change in HTML code and replace is False
    # @TODO: store in MongoDB

    config = get_config()

    if mdb_db is None:
        mdb_db = connect_mongodb()

    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    # select collection
    mdb_col = mdb_db[config.get('MONGODB', 'col_mep_register_copies')]

    # check if there already exists a MongoDB document with that MEP ID
    mongodb_mep_ids = list(mdb_col.find({"mep_id": mep_id},{"_id":1, "html": 1, "mep_id": 1, 'timestamp': 1}).sort("timestamp"))

    if len(mongodb_mep_ids) > 0:
        if timediff is not None:
            last_copy_timestamp = mongodb_mep_ids[-1].get("timestamp", 0)
            if (timestamp - last_copy_timestamp)>timediff:
                add_to_mongodb = True
            else:
                add_to_mongodb = False
        elif insert_only_update:
            # @TODO: check if there have been changes to the HTML
            pass
            add_to_mongodb = True
        else:
            add_to_mongodb = True
    else:
        replace = False
        add_to_mongodb = True

    if add_to_mongodb:
        mdb_doc = {'mep_id': mep_id, 'url': url, 'timestamp': timestamp, 'html': html}

        if replace: # update latest saved copy with current information
            mdb_operation = mdb_col.replace_one({'$query': {"mep_id": mep_id}, '$orderby': {'timestamp': -1}}, mdb_doc, upsert= True)
            if mdb_operation.modified_count > 1 or mdb_operation.upserted_id is not None:
                return True
            else:
                return False
        else: # insert new document
            mdb_operation = mdb_col.insert_one(mdb_doc)
            if mdb_operation.inserted_id is not None:
                return True
            else:
                return False
    else:
        return False

