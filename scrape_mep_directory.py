# Scrape the main directory of all MEPs for IDs and them to mep.db

import urllib3
import re
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(BASE_DIR))
import utilities

config = utilities.get_config()

http = urllib3.PoolManager()

# open MEP Directory
request = http.request('GET', config.get('GENERAL', 'mep_register_directory_xml_url'), redirect=5, retries=5)
xml = request.data.decode()

# get all ids
mep_ids = [(id,) for id in re.findall(r'(?<=<id>).*?(?=</id>)', xml, flags=re.MULTILINE)]

# insert all into DB
conn = utilities.connect_sqlite()
cur = conn.cursor()
cur.executemany('INSERT INTO meps (mep_id) VALUES (?);', (mep_ids))

# END