# @TODO: logging
# @TODO: parallelize
import datetime
import time
import joblib
from bs4 import BeautifulSoup
import os
import sys
import re
import argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(BASE_DIR))
import utilities

config = utilities.get_config()

os.environ["TZ"] = "UTC"

# set up logging
log = utilities.log_to_file(os.path.join(BASE_DIR, 'logs/parse_mep_pages_%s.txt') % (
    datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")), level="INFO")


# SQL connection
conn = utilities.connect_sqlite()
cur = conn.cursor()

#  db connection
mdb_db = utilities.connect_mongodb()
mdb_col = mdb_db[config.get('MONGODB', 'col_mep_register_copies')]

# for all unique urls in MongoDB get the latest, resp.
stored_mdb = mdb_col.aggregate([
  { "$sort": { "timestamp": -1 }},
  { "$group": {
    "_id": "$url",
    "doc": { "$first": "$$ROOT" }
  }},
  { "$replaceRoot": {
    "newRoot": "$doc"
  }}
], allowDiskUse = True)

# loop over all documents retrieved from mongodb
for doc in stored_mdb:

    log.info("Parsing document " + str(doc['_id']))

    mep_id = doc['mep_id']
    timestamp = doc['timestamp']
    url = doc['url']
    ep_num = int(re.findall(r'[0-9]+/*$', url)[0])
    html = doc['html']

    # @TODO: parse
    bs_obj = BeautifulSoup(html, "html.parser")
    mep_header = bs_obj.find(id="presentationmep")
    mep_term = bs_obj.find(id="detailedcardmep").find_next("section")

    # @TODO: Website version/timestamp sensitive parsing

    # header information parsing
    if mep_header is None:
        log.error(str(mep_id) + " " + url + " " + "MEP header html not found!")
    else:
        mep_name = mep_header.find(class_=re.compile(r'h1')).get_text().strip()

        mep_ms = mep_header.find(class_=re.compile(r'h3')).get_text().strip()

        mep_birthdate_tag = mep_header.find(id="birthDate")
        if mep_birthdate_tag is None:
            mep_birthdate = None
        else:
            if mep_birthdate_tag.get('datetime') is not None and len(mep_birthdate_tag.get('datetime')) > 0:
                mep_birthdate = mep_birthdate_tag.get('datetime')
            else:
                mep_birthdate = mep_birthdate_tag.get_text().strip() # @TODO parse as datetime

            try:
                mep_birthplace = mep_birthdate_tag.parent.get_text().strip().split(",")[1].strip()
            except:
                mep_birthplace = None

        mep_deathdate_tag = mep_header.find(id="deathDate")
        if mep_deathdate_tag is None:
            mep_deathdate = None
        else:
            if mep_deathdate_tag.get('datetime') is not None and len(mep_deathdate_tag.get('datetime')) > 0:
                mep_deathdate = mep_deathdate_tag.get('datetime')
            else:
                mep_deathdate = mep_deathdate_tag.get_text().strip()  # @TODO parse as datetime


    # term activity parsing
    if mep_term is None:
        log.error(str(mep_id) + " " + url + " " + "MEP term activity html not found!")
    else:
        pass

    # @TODO: save parsed information to attribute table
