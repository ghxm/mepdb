# collection functions used across scripts

import configparser
import sqlite3
import os
import sys
import logging
import datetime
import warnings

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, os.path.abspath(BASE_DIR))

def log_to_file(filename, level=logging.INFO):
    """
    This function sets up a logger that writes to a file.

    Parameters:
    filename (str): The name of the file to which the log will be written.
    level (int, optional): The level of the log. Defaults to logging.INFO.

    Returns:
    logger: A logger instance.
    """

    handler = logging.FileHandler (filename)
    handler.setLevel (level)
    formatter = logging.Formatter (fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter (formatter)
    loggername = filename
    logger = logging.getLogger (loggername)
    logger.setLevel (level)
    logger.addHandler (handler)

    return logger


def get_config(path = None):
    """
    This function reads the configuration file.

    Parameters:
    path (str, optional): The path to the configuration file. If not provided, the function will look for 'config.ini' in the current directory.

    Returns:
    config: A ConfigParser instance that holds the configuration data.
    """

    config = configparser.ConfigParser ()

    if path is None:
        config_path = os.path.abspath (os.path.dirname (__file__)) + '/../'
    else:
        config_path = path

    if not config_path.endswith (".ini"):
        if "config.ini" in os.listdir(config_path):
            path = config_path + "/config.ini"
        else:
            raise Exception("config.ini not found!")
    else:
        path = config_path

    config.read (path, encoding = 'utf-8')

    return config


def connect_sqlite(db_path = None, check_same_thread = False, not_exists_create = False):
    """
    This function establishes a connection to an SQLite database.

    Parameters:
    db_path (str, optional): The path to the SQLite database file. If not provided, the function will use the path specified in the configuration file.
    check_same_thread (bool, optional): If set to False, allows multiple threads to use the same connection. Defaults to False.
    not_exists_create (bool, optional): If set to True, a new database file will be created if it does not exist. Defaults to False.

    Returns:
    conn: A SQLite connection instance.
    """

    config = get_config()

    if db_path is None:
        db_path = os.path.join(BASE_DIR, config.get ('SQL', 'sqlite_db'))

    if os.path.exists(db_path):
        pass
    else:
        if not_exists_create:
            warnings.warn("DB file ("+ db_path +") does not exist! Creating new DB file.")
        else:
            raise Exception("DB file ("+ db_path +") does not exist!")

    conn = sqlite3.connect (database=db_path, check_same_thread=check_same_thread)

    conn.isolation_level = None

    return conn


def add_mep_html_sqlite(html, mep_id, url=None, timestamp=None, insert_only_update=True, timediff: datetime.timedelta=None, replace=False):
    """
    This function is used to add or update HTML content associated with a specific MEP (Member of the European Parliament) ID and URL into an SQLite database.

    Parameters:
    html (str): The HTML content that you want to add or update in the SQLite database.
    mep_id (int): The ID of the MEP. This ID is used to identify the specific record in the SQLite database.
    url (str, optional): The URL associated with the MEP ID. If provided, it is used to identify the specific record in the SQLite database.
    insert_only_update (bool, optional): If set to True (default), a new record will only be inserted if the HTML content has changed. If set to False, a new record will be inserted regardless of whether the HTML content has changed.
    timediff (datetime.timedelta, optional): If provided, a new record will only be inserted if the timestamp of the most recent record is older than this timedelta.
    timestamp (datetime, optional): If provided, it is used as the timestamp for the new record. If not provided, the current time (in UTC) is used.
    replace (bool, optional): If set to True, the existing record with the same MEP ID and URL will be updated with the new HTML content and timestamp. If set to False (default), a new record will be inserted.

    Returns:
    None
    """

    # Connect to SQLite database
    conn = connect_sqlite()
    c = conn.cursor()

    # Set the timestamp to current time if not provided
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    # Check if there already exists a record with that MEP ID and url
    c.execute('SELECT * FROM mep_data WHERE mep_id = ? AND url = ? ORDER BY timestamp DESC', (mep_id, url))
    record = c.fetchone()

    if record:
        try:
            last_copy_timestamp = datetime.datetime.strptime(record[2], "%Y-%m-%d %H:%M:%S.%f")
        except:
            last_copy_timestamp = None

        insert = False

        if (timediff is not None and timestamp is not None and last_copy_timestamp is not None) and (timestamp - last_copy_timestamp) > timediff:
            insert = True

        if insert_only_update:
            # Insert a new record only if the HTML content has changed
            if html != record[3]:
                insert = True
            else:
                insert = False


        # if there is reason to add the new html
        if insert:
            if replace:
                # Update the existing record with the new HTML content and timestamp
                c.execute('UPDATE mep_data SET html = ?, timestamp = ? WHERE id = ?', (html, timestamp, mep_id, url, record[0]))
            else:
                # Insert a new record
                c.execute('INSERT INTO mep_data (mep_id, url, timestamp, html) VALUES (?, ?, ?, ?)',
                          (mep_id, url, timestamp, html))

    else:
        # Insert a new record
        c.execute('INSERT INTO mep_data (mep_id, url, timestamp, html) VALUES (?, ?, ?, ?)', (mep_id, url, timestamp, html))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

