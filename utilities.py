# collection functions used across scripts

import configparser
import os
import sys
import re
import mongo_proxy
script_path = os.path.abspath(re.sub(r'(?<=mepdb\/).*', "", os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.abspath(script_path))

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