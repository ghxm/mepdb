# test and download MEP sites on the EU register by bruteforcing MEP IDs
# @TODO: Log Stats
# @TODO: Save to MongoDB

import urllib3
import sqlite3
import argparse
import datetime
import joblib
import time

import os
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(BASE_DIR))
import utilities

os.environ["TZ"] = "UTC"

# set up logging
log = utilities.log_to_file (os.path.join (BASE_DIR, 'logs/bruteforce_mep_download_%s.txt') % (
    datetime.datetime.utcnow ().strftime ("%Y%m%d%H%M%S")), level = "INFO")

# command line args
parser = argparse.ArgumentParser ()
parser.add_argument ("-p", "--parallel", action="store_true", default=False)
parser.add_argument ("-v", "--verbose", action="count", default=0, help = "prints out iterations in parallel processing")
parser.add_argument ("-n", "--njobs", default="auto")
parser.add_argument ("-d", "--download", action="store_true", default=False) # Download MEP pages and save to MongoDB
parser.add_argument ("--onlynew", action="store_true", default=False, help="automatically set lower ID range to highest ID in DB")
parser.add_argument ("--lower", type = int, default=0)
parser.add_argument ("--upper", type = int, default=999999)
parser.add_argument("-l", "--limit", type = int, default=0)
parser.add_argument ("-w", "--wait", type=int, help = "wait in seconds between requests", default=1)
args = parser.parse_args()

log.info (args)

if args.parallel:
    try:
        args.njobs = int(args.njobs)
    except:
        if args.njobs == "auto":
            args.njobs = int(joblib.cpu_count ())*10-5
        else:
            raise (Exception("No valid value for --njobs supplied"))

config = utilities.get_config()

# MongoDB connection
#utilities.connect_mongodb()
#mdb_db = utilities.connect_mongodb()
#mdb_col = mdb_db[config.get('MONGODB', '')]

# SQL connection
conn = utilities.connect_sqlite()

headers = {
    'user-agent': 'EUPLEX-MEPDB webspider (spiders@euplex.org)', # to identify the webspider vis a vis the server
    'accept-language': 'en-gb'}

if args.onlynew:
    cur = conn.cursor()
    db_mep_ids  = [i[0] for i in cur.execute('SELECT mep_id FROM meps').fetchall()]
    db_mep_ids.sort()
    if len(db_mep_ids) != 0:
        args.lower = db_mep_ids[-1]


if args.limit > 0:
    args.upper = args.lower + args.limit
elif args.limit < 0:
    raise Warning("negative --limit flag equals 0 (no limit)")


id_range = range(args.lower, args.upper)

http = urllib3.PoolManager()

def pipeline(id):

    html = None
    url = None

    # save ID to mep.db
    url = config.get('GENERAL', 'mep_register_base_url').format(id=str(id))
    # request
    log.info('ID ' + str(id) + ': requesting ' + str(url))
    timestamp = datetime.datetime.utcnow ()
    request = http.request('GET', url, redirect=5, retries=5, headers=headers)

    log.info('ID ' + str(id) + ': Status ' + str(request.status))

    if 399 < int(request.status) < 500:
        pass
    elif int(request.status) == 200:
        log.info('ID ' + str(id) + ': successful request')
        # save id to mep.db
        try:
            cur = conn.cursor()
            log.info('ID ' + str(id) + ': adding to SQLITE DB...')
            cur.execute('INSERT INTO meps (mep_id) VALUES (?);', (id,))
            new = True
        except sqlite3.IntegrityError:
            log.info('ID ' + str(id) + ': already in SQLITE DB')
            new = False

        # save HTML to MongoDB
        if new and args.download:
            utilities.add_mep_html_mongodb(html = request.data.decode (), url = url, timediff=datetime.timedelta(days=30))
    else:
        log.info('ID ' + str(id) + ': unhandled status code' + str(request.status) + ", ID: " + str(id))

    time.sleep(args.wait)

if not args.parallel:
    for id in id_range:
        pipeline(id)
else:
    # run parallelized version
    joblib.Parallel(require = "sharedmem", n_jobs=args.njobs, verbose=args.verbose)(joblib.delayed(pipeline)(id) for id in id_range)
