# Scrape the main directory of all MEPs for IDs and them to mep.db

import urllib3
import re
import os
import sys
import sqlite3

from src import utilities
from utilities import BASE_DIR

config = utilities.get_config()

http = urllib3.PoolManager()

# open MEP Directory
request = http.request('GET', config.get('GENERAL', 'mep_register_directory_xml_url'), redirect=5, retries=5)
xml = request.data.decode()

# get all ids
mep_ids = [(id,) for id in re.findall(r'(?<=<id>).*?(?=</id>)', xml, flags=re.MULTILINE)]

print(str(len(mep_ids)) + " MEP IDs found")

# insert all into DB

conn = utilities.connect_sqlite()
cur = conn.cursor()

new_ids = 0
ex_ids = 0

for mep_id in mep_ids:
    try:
        cur.execute('INSERT INTO meps (mep_id) VALUES (?);', (mep_id))
        new_ids = new_ids + 1
    except sqlite3.IntegrityError:
        ex_ids = ex_ids + 1

print(str(new_ids) + " new IDs")
print(str(ex_ids) + " existing IDs")
print(str(new_ids) + " new IDs written to mepdb")



# END