# Download MEP pages and save to Sqlite

import datetime
import urllib3
import time
import joblib
import os
import sys
import re
import argparse
from tqdm import tqdm

from src import utilities
from utilities import BASE_DIR

os.environ["TZ"] = "UTC"

# set up logging
log = utilities.log_to_file(os.path.join(BASE_DIR, 'logs/mep_download_%s.txt') % (
    datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")), level="INFO")

# command line args
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--parallel", action="store_true", default=False)
parser.add_argument("-v", "--verbose", action="count", default=0, help="prints out iterations in parallel processing")
parser.add_argument("-n", "--njobs", default="auto")
parser.add_argument('-e','--ep', nargs='+', help='EP numbers to download', default=range(1,10), required=False)
parser.add_argument("-r", "--replace", action="store_true", default=False)  # Replace existing MEP pages in SQLite instead of adding new source entries
parser.add_argument("--days", type=int, default=30, help="only update if last update is older than x days")
parser.add_argument("-u", "--update", action="store_true", default=False,
                    help="Re-download all and update existing MEP pages in SQLite. If false, only try to download new MEP pages and those that have been downloaded more than --days ago (if --days is set).")
parser.add_argument("-l", "--limit", type=int, default=None)
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
else:
    args.njobs = 1
if not args.update:
    args.days = None

if args.days is not None:
    args.days = datetime.timedelta(args.days)

config = utilities.get_config()

# SQL connection
conn = utilities.connect_sqlite()
cur = conn.cursor()

headers = {
    'user-agent': 'EUPLEX-MEPDB webspider (spiders@euplex.org)',  # to identify the webspider vis a vis the server
    'accept-language': 'en-gb'}

# Only select those not downloaded in the last x days if args.days is not None
db_meps = list([i for i in cur.execute('SELECT mep_id, url_name FROM meps').fetchall()])

ep_nums = args.ep

# create potential URL inputs
db_mep_eps = [mep + (ep_num,) for mep in db_meps for ep_num in ep_nums][0:args.limit]


if not args.update: # keep only not yet downloaded URLs to download

    # get all already stored MEP html from SQLite
    stored_mep_html = list([i for i in cur.execute('SELECT mep_id, url, html, timestamp FROM mep_data').fetchall()])

    if not args.days:
        # keep only urls that have been downloaded in the last x days
        stored_mep_html = [m for m in stored_mep_html if m[3] > datetime.datetime.utcnow() - args.days]

    # transform stored_mep_html to list of tuples (mep_id, url_name, ep_num) for comparison
    stored_mep_compare = [(m[0], re.findall(r'(?<=[0-9]/).*?(?=/)',m[1])[0], int(re.findall(r'[0-9]+/*$', m[1])[0])) for m in stored_mep_html]

    # remove already stored MEP html from download list
    mep_ids_eps = {x for x in set(db_mep_eps) if (x[0], x[2]) not in {(m[0], m[2]) for m in set(stored_mep_compare)}}
else:
    # get all (updates old html) MEP html pages from SQLite
    mep_ids_eps = db_mep_eps

if args.limit is not None:
    mep_ids_eps = mep_ids_eps[0:args.limit]

log.info(str(len(mep_ids_eps)) + " MEP - EP combinations selected")

# Close the cursor
cur.close()

# Close the connection
conn.close()

http = urllib3.PoolManager()

try_again = False

application_unavailable_timeout = False

def pipeline(id, url_name, ep_num):
    global application_unavailable_timeout

    # Create a new SQLite connection and cursor for this process
    conn = utilities.connect_sqlite()
    cur = conn.cursor()

    if application_unavailable_timeout:  # the MEP register is not available stop all processes for 5 mintues
        time.sleep(5 * 60)

    try_again = True
    trials = 0
    while try_again:

        time.sleep(trials)

        is_valid_html = False

        trials = trials + 1

        try_again = False

        html = None
        url = None

        url_name_tries = 0
        while url_name is None:

            name_request = http.request('GET', config.get('GENERAL', 'mep_register_base_url').format(id=str(id)), redirect=10, retries=10, headers=headers)

            landing_url_name = name_request.geturl()

            url_name_tries = url_name_tries + 1

            try:
                # get url name
                url_name = re.findall(r'(?<=[0-9]/).*?(?=/)', landing_url_name)[0]
            except:
                if url_name_tries > 5:
                    log.warning('ID ' + str(id) + " " + str(ep_num) + ': url_name not found')
                    break
                else:
                    time.sleep(1+url_name_tries)
                    url_name = None

            if url_name is not None:
                # Create a new SQLite connection and cursor for this process
                conn = utilities.connect_sqlite()
                cur = conn.cursor()
                # add to SQLite
                cur.execute('UPDATE meps SET url_name = ? WHERE mep_id = ?;', (url_name, id))
                conn.commit()
                conn.close()


        if url_name is None:
            log.error('ID ' + str(id) + " " + str(ep_num) + ': url_name is None')
            break

        # save ID to mep.db
        url = config.get('GENERAL', 'mep_scrape_url').format(id=str(id), url_name=url_name, ep_num=str(ep_num))
        # request
        log.info('ID ' + str(id) + " " + str(ep_num) + ': requesting ' + str(url))
        timestamp = datetime.datetime.utcnow()
        request = http.request('GET', url, redirect=5, retries=5, headers=headers)

        log.info('ID ' + str(id) + ': Status ' + str(request.status))

        landing_url = request.geturl()

        landing_url_num_list = re.findall(r'[0-9]+/*$', landing_url)

        if len(landing_url_num_list) != 0:
            landing_url_num = int(landing_url_num_list[0])
        else:
            landing_url_num = 0

        if landing_url_num != ep_num or int(request.status) == 303 or int(request.status) == 404:
            log.info('ID ' + str(id) + " " + str(ep_num) + ': Not a valid EP number for this MEP')
            is_valid_html = False
            try_again = False
            break
        elif int(request.status) == 429:
            log.warning('ID ' + str(id) + " " + str(ep_num) + ': too many requests, waiting for 2 minutes...')
            time.sleep(2 * 60)
            try_again = True
            is_valid_html = False
        elif 399 < int(request.status) < 500:
            is_valid_html = False
            try_again = False
        elif int(request.status) == 200:
            html = request.data.decode()
            if len(html) < 1200 or re.search(r'Application\s*unavailable', html) is not None:
                if trials > 30:
                    try_again = False
                else:
                    try_again = True
                    application_unavailable_timeout = True
                    log.warning('ID ' + str(id) + " " + str(ep_num) + ": MEP register unavailable, waiting for 2 minutes...")
                    time.sleep(2 * 60)
                    continue
            else:
                is_valid_html = True
        else:
            log.info('ID ' + str(id) + " " + str(ep_num) + ': unhandled status code' + str(request.status))
            try_again = True
            is_valid_html = False

        log.info('ID ' + str(id) + " " + str(ep_num) +  ': successful request (Status code 200)')


        # save HTML to MongoDB
        if is_valid_html:
            log.info('ID ' + str(id) + " " + str(ep_num) + ': adding to SQLite...')
            utilities.add_mep_html_sqlite(html=request.data.decode(), mep_id=id, url=url,
                                           timediff=args.days, replace=args.replace)
            break

        if trials > 30:
            log.error("MEP register appears to be down. Shutting down (trials > 30)...")
            sys.exit(0)
        elif trials > 29:
            log.warning("Wait for 30 minutes (trials > 29)")
            time.sleep(60*30)

        time.sleep(args.wait)


joblib.Parallel(require="sharedmem", n_jobs=args.njobs, verbose=args.verbose)(
        joblib.delayed(pipeline)(id, url_name, ep_num) for id, url_name, ep_num in tqdm(mep_ids_eps))
