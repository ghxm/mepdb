# download and add MEP pages to MongoDB from various sources (SQL, file, ...)
# @TODO: Test

import datetime
import urllib3
import time
import joblib
import os
import sys
import re
import argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(BASE_DIR))
import utilities

os.environ["TZ"] = "UTC"

# set up logging
log = utilities.log_to_file(os.path.join(BASE_DIR, 'logs/collect_mep_url_names_%s.txt') % (
    datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")), level="INFO")

# command line args
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--parallel", action="store_true", default=False)
parser.add_argument("-v", "--verbose", action="count", default=0, help="prints out iterations in parallel processing")
parser.add_argument("-n", "--njobs", default="auto")
parser.add_argument("-r", "--replace", action="store_true", default=False)  # Download MEP pages and save to MongoDB
parser.add_argument("-w", "--wait", type=int, help="wait in seconds between requests", default=1)
args = parser.parse_args()

log.info(args)

if args.parallel:
    try:
        args.njobs = int(args.njobs)
    except:
        if args.njobs == "auto":
            args.njobs = int(joblib.cpu_count()) * 5 - 5
        else:
            raise (Exception("No valid value for --njobs supplied"))

config = utilities.get_config()

# SQL connection
conn = utilities.connect_sqlite()
cur = conn.cursor()

headers = {
    'user-agent': 'EUPLEX-MEPDB webspider (spiders@euplex.org)',  # to identify the webspider vis a vis the server
    'accept-language': 'en-gb'}

sql_mep_ids_query = 'SELECT mep_id FROM meps'
if not args.replace:
    sql_mep_ids_query = sql_mep_ids_query + ' WHERE url_name is null'
db_mep_ids = list([i[0] for i in cur.execute(sql_mep_ids_query).fetchall()])

log.info(str(len(db_mep_ids)) + " IDs selected")

http = urllib3.PoolManager()

try_again = False

application_unavailable_timeout = False

def pipeline(id):
    global application_unavailable_timeout

    if application_unavailable_timeout:  # the MEP register is not available stop all processes for 5 mintues
        time.sleep(5 * 60)

    try_again = True
    trials = 0
    url_name = None

    while try_again:

        time.sleep(args.wait)

        is_valid_html = None

        trials = trials + 1

        try_again = False

        html = None
        url = None
        url_name = None

        # save ID to mep.db
        url = config.get('GENERAL', 'mep_register_base_url').format(id=str(id))
        # request
        log.info('ID ' + str(id) + ': requesting ' + str(url))
        timestamp = datetime.datetime.utcnow()
        request = http.request('GET', url, redirect=5, retries=5, headers=headers)

        log.info('ID ' + str(id) + ': Status ' + str(request.status))

        if 399 < int(request.status) < 500:
            is_valid_html = False
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
                is_valid_html = True
                try_again = False
        else:
            log.info('ID ' + str(id) + ': unhandled status code' + str(request.status) + ", ID: " + str(id))

        log.info('ID ' + str(id) + ': successful request (Status code 200)')

        # get url_name
        mep_url = request.geturl()
        try:
            url_name = re.findall(r'(?<=[0-9]/).*?(?=/)', mep_url)[0]
        except:
            log.error('ID ' + str(id) + ' no url_name found in url ' + str(mep_url))
            break

        if url_name is not None:
            break

        if trials > 30:
            log.error("MEP register appears to be down. Shutting down (trials > 30)...")
            sys.exit(0)
        elif trials > 29:
            log.warning("Wait for 30 minutes (trials > 29)")
            time.sleep(60*30)

    try:
        cur = conn.cursor()
        log.info('ID ' + str(id) + ': adding to SQLITE DB...')
        cur.execute('UPDATE meps SET url_name = ? WHERE mep_id = ?;', (url_name, id))
    except Exception as e:
        log.error('ID ' + str(id) + ': could not add to database: ' + str(e))

if not args.parallel:
    for id in db_mep_ids:
        pipeline(id)
else:
    # run parallelized version
    joblib.Parallel(require="sharedmem", n_jobs=args.njobs, verbose=args.verbose)(
        joblib.delayed(pipeline)(id) for id in db_mep_ids)
