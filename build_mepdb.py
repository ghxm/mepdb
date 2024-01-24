import sqlite3

from src import utilities
import os
import subprocess
from tqdm import tqdm

def run_script(script_name, args = []):
    """
    Run a python script from the src directory
    :param script_name: name of the script
    :param tabbed_output: print output with tabbed indentation
    :return: None
    """
    global script_dir

    print("\033[92mRunning " + script_name + "\033[0m")

    subprocess.run(['python', os.path.join(script_dir, script_name), *args])


# path of this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Get the absolute path to the directory containing your scripts
script_dir = os.path.abspath(os.path.join(BASE_DIR, 'src'))

config = utilities.get_config(BASE_DIR)

# Check if SQLite DB exists
db_path = os.path.join(BASE_DIR, config.get('SQL', 'sqlite_db'))

# create empty new DB file
conn = utilities.connect_sqlite(db_path, not_exists_create=True)
cur = conn.cursor()

try:
    # Read the SQL file
    with open('mep.db.sql', 'r') as file:
        sql_script = file.read()

    # Execute the SQL script
    cur.executescript(sql_script)
except sqlite3.OperationalError:
    print("Database already exists")

# Start by scraping the MEP directory
run_script("scrape_mep_directory.py")

# Collect MEP url names from the MEP register to make it easier to scrape MEP pages
run_script("collect_mep_url_names.py")

# Download MEP pages and save to SQLite
run_script("download_mep_html.py", ["-p", "-n 3"])

# Parse MEP pages
run_script("parse_mep_html.py", ['--csv'])

