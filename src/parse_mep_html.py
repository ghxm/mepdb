# @TODO: command-line arguments
import datetime
from bs4 import BeautifulSoup
import os
import sys
import re
import pandas as pd
import argparse
from tqdm import tqdm
import joblib

from src import utilities
from utilities import BASE_DIR

config = utilities.get_config()

# command line args
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--parallel", action="store_true", default=False, help="Parallel processing")
parser.add_argument("-v", "--verbose", action="count", default=0, help="Print out iterations in parallel processing")
parser.add_argument("-n", "--njobs", default="auto", help="Number of parallel jobs")
parser.add_argument("-c", "--csv", action="store_true", default=False, help="write output to csv as well")
args = parser.parse_args()


if args.parallel:
    try:
        args.njobs = int(args.njobs)
    except:
        if args.njobs == "auto":
            args.njobs = int(joblib.cpu_count())
        else:
            raise (Exception("No valid value for --njobs supplied"))



os.environ["TZ"] = "UTC"

# set up logging
log = utilities.log_to_file(os.path.join(BASE_DIR, 'logs/parse_mep_pages_%s.txt') % (
    datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")), level="INFO")


# SQL connection
conn = utilities.connect_sqlite()
cur = conn.cursor()

# get all sources from SQLite (if there are multiple for the same URL, only the most recent one is kept)
stored_mep_html = list(cur.execute("""
SELECT s1.mep_id, s1.url, s1.timestamp, s1.html
FROM mep_data s1
JOIN (
    SELECT url, MAX(timestamp) as max_timestamp
    FROM mep_data
    GROUP BY url
) s2
ON s1.url = s2.url AND s1.timestamp = s2.max_timestamp;
""").fetchall())

# close connection
cur.close()
conn.close()

# @DEBUG
# stored_mdb = mdb_col.find({'url': "https://www.europarl.europa.eu/meps/en/1/GEORG_JARZEMBOWSKI/history/3"})

eu_countries = ["Austria","Belgium", "Bulgaria","Croatia","Cyprus",
                  "Czech Republic","Czechia","Denmark","Estonia","Finland","France","Germany",
                  "Greece","Hungary","Ireland","Italy","Latvia","Lithuania",
                  "Luxembourg", "Malta","Netherlands", "Poland","Portugal","Romania",
                  "Slovakia","Slovenia","Spain","Sweden", "United Kingdom"]
eu_regex = "|".join([str(country) for country in eu_countries])


# loop over all documents retrieved from mongodb
def pipeline(doc):

    mep_attributes = {}

    log.info("Parsing document " + str(doc[1]))

    mep_attributes['mep_id'] = doc[0]
    mep_attributes['timestamp'] = doc[2]
    mep_attributes['url'] = doc[1]
    mep_attributes['ep_num'] = int(re.findall(r'[0-9]+/*$', mep_attributes['url'])[0])
    html = doc[-1]

    # parse
    bs_obj = BeautifulSoup(html, "html.parser")
    mep_header = bs_obj.find(id="presentationmep")
    mep_term = bs_obj.find(id="detailedcardmep").find_next("section")

    # @TODO: Website version/timestamp sensitive parsing

    # header information parsing
    if mep_header is None:
        log.error(str(mep_attributes['id']) + " " + mep_attributes['url'] + " " + "MEP header html not found!")
    else:
        try:
            mep_attributes['name'] = mep_header.find(class_=re.compile(r'h1')).get_text().strip()
            if len(mep_attributes['name']) == 0:
                raise Exception
        except:
            mep_attributes['name'] = None

        try:
            h3_ms_tags = mep_header.find_all(class_=re.compile(r'h3'), string=re.compile(eu_regex, flags=re.IGNORECASE))
            if len(h3_ms_tags) == 0:
                raise Exception
            else:
                mep_attributes['ms'] = re.search(eu_regex, h3_ms_tags[0].get_text().strip(), flags=re.IGNORECASE)[0]
        except:
            mep_attributes['ms'] = None

        mep_date_birth_tag = mep_header.find(class_=re.compile(r'(birth.date|date.birth)'))
        if mep_date_birth_tag is not None:

            if mep_date_birth_tag.get('datetime') is not None and len(mep_date_birth_tag.get('datetime')) > 0:
                mep_attributes['date_birth'] = mep_date_birth_tag.get('datetime')
            else:
                mep_attributes['date_birth'] = mep_date_birth_tag.get_text().strip()

        mep_place_birth_tag = mep_header.find(class_=re.compile(r'(birth.place|place.birth)'))
        try:
            mep_attributes['place_birth'] = mep_place_birth_tag.get_text().strip()
        except:
            mep_attributes['place_birth'] = None

        mep_date_death_tag = mep_header.find(class_=re.compile(r'(death.date|date.death)'))
        if mep_date_death_tag is not None:
            if mep_date_death_tag.get('datetime') is not None and len(mep_date_death_tag.get('datetime')) > 0:
                mep_attributes['date_death'] = mep_date_death_tag.get('datetime')
            else:
                mep_attributes['date_death'] = mep_date_death_tag.get_text().strip()  # @TODO parse as datetime

    list_mep_roles = []

    # role parsing
    # role parsing
    if mep_term is None:
        log.error(str(mep_attributes['id']) + " " + mep_attributes['url'] + " " + "MEP term activity html not found!")
    else:
        mep_term_status = mep_term.find(id = "status")
        if mep_term_status is not None:
            # loop over class="erpl_meps-status"
            for section in mep_term_status.find_all(class_="erpl_meps-status"):
                section_name = section.find("h4").get_text().strip().lower().replace(" ", "_")
                for li in section.find_all("li"):
                    role_item = {'mep_id': mep_attributes['mep_id'],
                                 'url': mep_attributes['url'],
                                 'timestamp' : mep_attributes['timestamp'],
                                 'ep_num': mep_attributes['ep_num']
                                 }
                    # try 2 versions to account for variance in formatting, one by format, one by syntax
                    strong_text = li.find("strong").get_text().strip()
                    normal_text = li.find("strong").nextSibling.strip()
                    format_sep_text = [strong_text, normal_text]
                    dash_sep_texts = li.get_text().strip().split(":", maxsplit = 1)
                    # Start/end date
                    try:
                        ## start with format text sep
                        role_item['date_start'] = None
                        role_item['date_end'] = None
                        if strong_text is not None and len(strong_text>0):
                            dates = re.findall(r'(?:[0-9]+-[0-9]+-[0-9]+)|(?:\.\.\.)', strong_text)
                            role_item['date_start'] = dates[0]
                            role_item['date_end'] = dates[1]
                        else:
                            raise Exception
                    except:
                        ## try syntax text sep
                        if dash_sep_texts is not None and any([len(t) > 0 for t in dash_sep_texts]):
                            dates = re.findall(r'(?:[0-9]+-[0-9]+-[0-9]+)|(?:\.\.\.)', dash_sep_texts[0])
                            try:
                                if role_item['date_start'] is None:
                                    role_item['date_start'] = dates[0]
                            except:
                                pass
                            try:
                                if role_item['date_end'] is None:
                                    role_item['date_end'] = dates[1]
                            except:
                                pass

                    # entity
                    try:
                        entity_text = None
                        if normal_text is not None and len(normal_text)>0:
                            if normal_text.startswith(":"):
                                entity_text = normal_text[1:]
                            else:
                                entity_text = normal_text
                        else:
                            raise Exception
                    except:
                        try:
                            if len(dash_sep_texts[1]) > 0:
                                entity_text = dash_sep_texts[1]
                        except:
                            pass
                    role_item['role'] = None
                    role_item['entity'] = None
                    role_item['type'] = None
                    if section_name == "political_groups" or section_name=="national_parties":
                        entity_type_dict = {"political_groups": "political group",
                                       "national_parties": "national party"}
                        role_item['type'] = entity_type_dict[section_name]
                        # get role from entity text
                        if role_item['type'] == "political group":
                            entity_split = entity_text.rsplit("- ", maxsplit = 1)
                            role_item['entity'] = entity_split[0].strip()
                            try:
                                role_item['role'] = entity_split[1].lower().strip()
                            except:
                                pass
                            #if role_item['role'] is None:
                            #    role_item['role'] = "member"
                        if role_item['type'] == "national party":
                            role_item['role'] = "member"
                            role_item['entity'] = entity_text.strip()
                            no_party_terms = ["parteilos", "no party", "sans étiquette", "sem partido", "independent", "independente", "indipindente" , "independiente", "indépendant", "Løsgænger", "Onafhankelijk", "Onafhankelijk lid", "Non party", "NEZÁVISLÍ"]
                            if any([re.search(no_party + "\s*[-]*\s*(?:\(|$)", role_item['entity'], flags=re.IGNORECASE) is not None for no_party in no_party_terms]) or\
                                    ((role_item['entity'].strip().startswith("(") or role_item['entity'].strip().startswith("-")) and role_item['entity'].strip().endswith(")")):
                                role_item['entity'] = re.sub("|".join(no_party_terms), "", role_item['entity'], flags=re.IGNORECASE)
                                role_item['entity'] = "None" + role_item['entity'].replace("-", " " , 1).replace("  ", " ")

                    else:
                        role_item['role'] = section_name.strip()
                        role_item['entity'] = entity_text.strip()
                        # get type from entity text
                        try:
                            role_item['type'] = re.findall(r'(?:committee|delegation|bureau)', entity_text, flags=re.IGNORECASE)[0].strip().lower()
                        except:
                            pass
                        if role_item['type'] is None:
                            role_item['type'] = "other"

                    list_mep_roles.append(role_item)

    return mep_attributes, list_mep_roles

    # TODO: parse other sections

results = joblib.Parallel(require='sharedmem', n_jobs=args.njobs, verbose=args.verbose)(joblib.delayed(pipeline)(doc) for doc in tqdm(stored_mep_html))

list_mep_attributes = [res[0] for res in results]
list_mep_roles = []

for res in results:
    list_mep_roles.extend(res[1])

log.info ("Creating pandas dataframes...")

df_attributes = pd.DataFrame.from_records(list_mep_attributes)

# Add missing columns
for col in ['mep_id', 'timestamp', 'name', 'ms', 'date_birth', 'place_birth', 'date_death']:
    if col not in df_attributes.columns:
        df_attributes[col] = pd.NA

# sort by timestamp and remove duplicates
log.info("removing duplicate MEP attributes and keeping only the most recently downloaded")
df_attributes.sort_values('timestamp', ascending = False, inplace=True)
df_attributes.drop_duplicates(subset=['mep_id'], keep='first', inplace=True)

df_roles = pd.DataFrame.from_records(list_mep_roles)

# sort by timestamp and remove duplicates
log.info("removing duplicate MEP attributes and keeping only the most recently downloaded")
df_roles.sort_values('timestamp', ascending = False, inplace=True)
df_roles.drop_duplicates(subset=['mep_id', 'role', 'entity', 'date_start', 'date_end'], keep='first', inplace=True)

for col in ['mep_id', 'url', 'timestamp', 'ep_num', 'date_start', 'date_end', 'role', 'entity', 'entity_type']:
    if col not in df_roles.columns:
        df_roles[col] = pd.NA



log.info ("Converting dataframe column types")
# convert date columns

def transform_date_columns(df):
    # Select columns that start with 'date_'
    date_columns = [col for col in df.columns if col.startswith('date_')]

    # Apply the transform_date_format function to each element of the selected columns
    for col in date_columns:
        df[col] = df[col].map(utilities.to_ymd_string)

    return df

# make sure we have Y-m-d format strings
df_attributes = transform_date_columns(df_attributes)
df_roles = transform_date_columns(df_roles)

df_attributes[["date_birth", "date_death"]] = df_attributes[["date_birth", "date_death"]].apply(pd.to_datetime, format='%Y-%m-%d', errors='coerce')
df_roles[["date_start", "date_end"]] = df_roles[["date_start", "date_end"]].apply(pd.to_datetime, format='%Y-%m-%d', errors='coerce')

# correct data where the start date is large than the end date
start_end_condition = (df_roles['date_end'].notnull()) & (df_roles['date_start'].notnull()) & (df_roles.date_start > df_roles.date_end)
date_start_temp = df_roles.loc[start_end_condition, "date_start"]
df_roles.loc[start_end_condition, "date_start"] = df_roles.loc[start_end_condition, "date_end"]
df_roles.loc[start_end_condition, "date_end"] = date_start_temp

# log.debug("Writing dataframes to csv for debugging")
if args.csv:
    df_attributes.to_csv(os.path.join(BASE_DIR, "data/attributes.csv"))
    df_roles.to_csv(os.path.join(BASE_DIR, "data/roles.csv"))

# df to sql, replace accordingly
try:
    # SQL connection
    conn = utilities.connect_sqlite()
    log.info("Writing attributes table to SQL DB...")
    df_attributes.to_sql("attributes", con = conn, if_exists='replace')
    log.info("... successfully written!")
    # close
    conn.close()
except Exception as e:
    log.error("... " + str(e))
try:
    # SQL connection
    conn = utilities.connect_sqlite()
    log.info("Writing roles table to SQL DB")
    df_roles.to_sql("roles", con = conn, if_exists='replace')
    log.info("... successfully written!")
    # close
    conn.close()
except Exception as e:
    log.error("... " + str(e))
