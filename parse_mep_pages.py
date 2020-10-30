# @TODO: command-line arguments
import datetime
import time
import joblib
from bs4 import BeautifulSoup
import os
import sys
import re
import pandas as pd
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

#  db connection
mdb_db = utilities.connect_mongodb()
mdb_col = mdb_db[config.get('MONGODB', 'col_mep_register_copies')]

# for all unique urls in MongoDB get the latest, resp.
stored_mdb = mdb_col.aggregate([
  { "$sort": { "timestamp": -1 }},
    #{"$limit": 100}, # @DEBUG
  { "$group": {
    "_id": "$url",
    "doc": { "$first": "$$ROOT" }
  }},
  { "$replaceRoot": {
    "newRoot": "$doc"
  }}
], allowDiskUse = True)

# @DEBUG
#stored_mdb = mdb_col.find({'url': "https://www.europarl.europa.eu/meps/en/4244/PATSY_SORENSEN/history/5"})

eu_countries = ["Austria","Belgium", "Bulgaria","Croatia","Cyprus",
                  "Czech Republic","Czechia","Denmark","Estonia","Finland","France","Germany",
                  "Greece","Hungary","Ireland","Italy","Latvia","Lithuania",
                  "Luxembourg", "Malta","Netherlands", "Poland","Portugal","Romania",
                  "Slovakia","Slovenia","Spain","Sweden", "United Kingdom"]
eu_regex = "|".join([str(country) for country in eu_countries])

list_mep_attributes = []
list_mep_roles = []

# loop over all documents retrieved from mongodb
for doc in stored_mdb:

    mep_attributes = {}

    log.info("Parsing document " + str(doc['_id']))

    mep_attributes['mep_id'] = doc['mep_id']
    mep_attributes['timestamp'] = doc['timestamp']
    mep_attributes['url'] = doc['url']
    mep_attributes['ep_num'] = int(re.findall(r'[0-9]+/*$', mep_attributes['url'])[0])
    html = doc['html']

    # @TODO: parse
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
            h3_ms_tags = mep_header.find_all(class_=re.compile(r'h3'), text=re.compile(eu_regex, flags=re.IGNORECASE))
            if len(h3_ms_tags) == 0:
                raise Exception
            else:
                mep_attributes['ms'] = re.search(eu_regex, h3_ms_tags[0].get_text().strip(), flags=re.IGNORECASE)[0]
        except:
            mep_attributes['ms'] = None

        mep_birthdate_tag = mep_header.find(id="birthDate")
        if mep_birthdate_tag is None:
            mep_birthdate = None
        else:
            if mep_birthdate_tag.get('datetime') is not None and len(mep_birthdate_tag.get('datetime')) > 0:
                mep_attributes['birthdate'] = mep_birthdate_tag.get('datetime')
            else:
                mep_attributes['birthdate'] = mep_birthdate_tag.get_text().strip() # @TODO parse as datetime

            try:
                mep_attributes['birthplace'] = mep_birthdate_tag.parent.get_text().strip().split(",")[1].strip()
            except:
                mep_attributes['birthplace'] = None

        mep_deathdate_tag = mep_header.find(id="deathDate")
        if mep_deathdate_tag is None:
            mep_attributes['deathdate'] = None
        else:
            if mep_deathdate_tag.get('datetime') is not None and len(mep_deathdate_tag.get('datetime')) > 0:
                mep_attributes['deathdate'] = mep_deathdate_tag.get('datetime')
            else:
                mep_attributes['deathdate'] = mep_deathdate_tag.get_text().strip()  # @TODO parse as datetime

    list_mep_attributes.append(mep_attributes)

    # @TODO: term activity parsing
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
                        role_item['start_date'] = None
                        role_item['end_date'] = None
                        if strong_text is not None and len(strong_text>0):
                            dates = re.findall(r'(?:[0-9]+-[0-9]+-[0-9]+)|(?:\.\.\.)', strong_text)
                            role_item['start_date'] = dates[0]
                            role_item['end_date'] = dates[1]
                        else:
                            raise Exception
                    except:
                        ## try syntax text sep
                        if dash_sep_texts is not None and any([len(t) > 0 for t in dash_sep_texts]):
                            dates = re.findall(r'(?:[0-9]+-[0-9]+-[0-9]+)|(?:\.\.\.)', dash_sep_texts[0])
                            try:
                                if role_item['start_date'] is None:
                                    role_item['start_date'] = dates[0]
                            except:
                                pass
                            try:
                                if role_item['end_date'] is None:
                                    role_item['end_date'] = dates[1]
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
                            no_party_terms = ["parteilos", "no party", "sans étiquette", "sem partido", "independent", "independente", "indipindente", "indépendant", "Løsgænger", "Onafhankelijk", "Onafhankelijk lid", "Non party", "NEZÁVISLÍ"]
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

log.info ("Creating pandas dataframes...")
# create empty pandas df
df_attributes = pd.DataFrame(columns=['mep_id', 'timestamp', 'name', 'ms', 'birthdate', 'birthplace', 'deathdate'])
df_roles = pd.DataFrame(columns=['mep_id', 'url', 'timestamp', 'ep_num', 'start_date', 'end_date', 'role', 'entity', 'entity_type'])


log.info ("Appending parsed information to dataframes...")
df_attributes = df_attributes.from_records(list_mep_attributes, exclude=['url', 'ep_num'])

# for attributes, sort by timestamp and remove duplicates
log.info("removing duplicate MEP attributes and keeping only the most recently downloaded")
df_attributes.sort_values('timestamp', inplace=True)
df_attributes.drop_duplicates(subset=['mep_id'], keep='first', inplace=True)

df_roles = df_roles.from_records(list_mep_roles)

# column types
log.info ("Converting dataframe column types")
## convert date columns
df_attributes[["birthdate", "deathdate"]] = df_attributes[["birthdate", "deathdate"]].apply(pd.to_datetime, errors='ignore')
df_roles[["start_date", "end_date"]] = df_roles[["start_date", "end_date"]].apply(pd.to_datetime, errors='ignore')

## @DEBUG: to csv
log.debug("Writing dataframes to csv for debugging")
df_attributes.to_csv(os.path.join(BASE_DIR, "dataset/attributes.csv"))
df_roles.to_csv(os.path.join(BASE_DIR, "dataset/roles.csv"))

# df to sql, replace accordingly
try:
    log.info("Writing attributes table to SQL DB...")
    df_attributes.to_sql("attributes", con = conn, if_exists='replace')
    log.info("... successfully written!")
except Exception as e:
    log.error("... " + str(e))
try:
    log.info("Writing roles table to SQL DB")
    df_roles.to_sql("roles", con = conn, if_exists='replace')
    log.info("... successfully written!")
except Exception as e:
    log.error("... " + str(e))
