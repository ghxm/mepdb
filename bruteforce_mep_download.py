# test and download MEP sites on the EU register by bruteforcing MEP IDs
# @TODO: Log Stats
# @TODO: Save to MongoDB

import urllib3
import sqlite3
import argparse
import datetime
import joblib
import time
import re

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(BASE_DIR))
import utilities

os.environ["TZ"] = "UTC"

# set up logging
log = utilities.log_to_file(os.path.join(BASE_DIR, 'logs/bruteforce_mep_download_%s.txt') % (
    datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")), level="INFO")

# command line args
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--parallel", action="store_true", default=False)
parser.add_argument("-v", "--verbose", action="count", default=0, help="prints out iterations in parallel processing")
parser.add_argument("-n", "--njobs", default="auto")
parser.add_argument("-d", "--download", action="store_true", default=False)  # Download MEP pages and save to MongoDB
parser.add_argument("--onlynew", action="store_true", default=False,
                    help="automatically set lower ID range to highest ID in DB")
parser.add_argument("--lower", type=int, default=0)
parser.add_argument("--upper", type=int, default=999999)
parser.add_argument("-l", "--limit", type=int, default=0)
parser.add_argument("-w", "--wait", type=int, help="wait in seconds between requests", default=1)
args = parser.parse_args()

log.info(args)

if args.parallel:
    try:
        args.njobs = int(args.njobs)
    except:
        if args.njobs == "auto":
            args.njobs = int(joblib.cpu_count()) * 10 - 5
        else:
            raise (Exception("No valid value for --njobs supplied"))

config = utilities.get_config()

# MongoDB connection
# utilities.connect_mongodb()
# mdb_db = utilities.connect_mongodb()
# mdb_col = mdb_db[config.get('MONGODB', '')]

# SQL connection
conn = utilities.connect_sqlite()

headers = {
    'user-agent': 'EUPLEX-MEPDB webspider (spiders@euplex.org)',  # to identify the webspider vis a vis the server
    'accept-language': 'en-gb'}

additional_ids = []

if args.onlynew:
    cur = conn.cursor()
    db_mep_ids = [i for i in cur.execute('SELECT mep_id, is_mep_id FROM meps').fetchall()]
    db_mep_ids.sort()
    if len(db_mep_ids) != 0:
        args.lower = db_mep_ids[-1][0]
        range_lower  = range(0, args.lower)
        existing_sqlite_ids = [id[0] for id in db_mep_ids]
        missing_from_sqlite = list(set(range_lower) - set(existing_sqlite_ids))
        ids_status_none = [id[0] for id in db_mep_ids if id[1] is None]
        additional_ids = ids_status_none + missing_from_sqlite

if args.limit > 0:
    args.upper = args.lower + args.limit
elif args.limit < 0:
    raise Warning("negative --limit flag equals 0 (no limit)")

id_range = list(range(args.lower, args.upper))
id_range = additional_ids + id_range

log.info(str(len(id_range)) + " IDs selected for requesting")
log.info("Additional IDs: " + str(additional_ids))


http = urllib3.PoolManager()

try_again = False

application_unavailable_timeout = False

def pipeline(id):
    global application_unavailable_timeout

    if application_unavailable_timeout:  # the MEP register is not available stop all processes for 5 mintues
        time.sleep(5 * 60)

    try_again = True
    trials = 0
    while try_again:

        is_mep_id = None

        trials = trials + 1

        try_again = False

        html = None
        url = None

        # save ID to mep.db
        url = config.get('GENERAL', 'mep_register_base_url').format(id=str(id))
        # request
        log.info('ID ' + str(id) + ': requesting ' + str(url))
        timestamp = datetime.datetime.utcnow()
        request = http.request('GET', url, redirect=5, retries=5, headers=headers)

        log.info('ID ' + str(id) + ': Status ' + str(request.status))

        if 399 < int(request.status) < 500:
            is_mep_id = False
        elif int(request.status) == 200:
            html = request.data.decode()
            if len(html) < 1200 or re.search(r'Application\s*unavailable', html) is not None:
                if trials > 30:
                    try_again = False
                else:
                    try_again = True
                    application_unavailable_timeout = True
                    log.warning('ID ' + str(id) + ": MEP register unavailable, waiting for 2 minutes...")
                    time.sleep(2 * 60)
                    continue
            else:
                is_mep_id = True
        else:
            log.info('ID ' + str(id) + ': unhandled status code' + str(request.status) + ", ID: " + str(id))

        log.info('ID ' + str(id) + ': successful request (Status code 200)')
        log.info('ID ' + str(id) + ': is MEP ID? ' + str(is_mep_id))

        # save id to mep.db
        try:
            cur = conn.cursor()
            log.info('ID ' + str(id) + ': adding to SQLITE DB...')
            cur.execute('INSERT INTO meps (mep_id, is_mep_id) VALUES (?, ?);', (id, is_mep_id))
            new = True
        except sqlite3.IntegrityError:
            log.info('ID ' + str(id) + ': already in SQLITE DB')
            new = False

        # save HTML to MongoDB
        if bool(is_mep_id) and new and args.download:
            utilities.add_mep_html_mongodb(html=request.data.decode(), mep_id=id, url=url,
                                           timediff=datetime.timedelta(days=30))


        if trials > 30:
            log.error("MEP register appears to be down. Shutting down (trials > 30)...")
            sys.exit(0)
        elif trials > 29:
            log.warning("Wait for 30 minutes (trials > 29)")
            time.sleep(60*30)

        time.sleep(args.wait)

if not args.parallel:
    for id in id_range:
        pipeline(id)
else:
    # run parallelized version
    joblib.Parallel(require="sharedmem", n_jobs=args.njobs, verbose=args.verbose)(
        joblib.delayed(pipeline)(id) for id in id_range)
