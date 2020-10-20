# test and download MEP sites on the EU register by bruteforcing MEP IDs
# @TODO: Logging
# @TODO: Save to MongoDB and SQL
# @TODO: Parallelization
# @TODO: command-line arguments

import re
import urllib3
import os
import sys
script_path = os.path.abspath(re.sub(r'(?<=mepdb\/).*', "", os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.abspath(script_path))
import utilities

config = utilities.get_config()

# MongoDB connection
utilities.connect_mongodb()
mdb_db = utilities.connect_mongodb()
mdb_col = mdb_db[config.get('MONGODB', '')]

headers = {
    'user-agent': 'EUPLEX-MEPDB webspider (spiders@euplex.org)',
    'accept-language': 'en-gb'}

id_range = range(0,999999)

http = urllib3.PoolManager()

# save ID to mep.db
for id in id_range:
    url = re.sub(r'%id%', str(id), config.get('GENERAL').get('base_url'))
    # request
    request = http.request('GET', url, redirect=5, retries=5, headers=headers)

    if 399 < int(request.status) < 500:
        continue
    else:
        # save id to mep.db
        # save HTML to MongoDB