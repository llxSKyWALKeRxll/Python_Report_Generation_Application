"""
Database service: Contains several methods which help in syncing the database with the CSV files,
run custom queries, fetch their results, etc.
"""

import csv
import threading
import time

import mysql.connector
from cryptography.fernet import Fernet
from mysql.connector import MySQLConnection, CMySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection


def get_db_credentials():
    """
    fetches database credentials for local database
    :return: a tuple consisting of the db host, db username, db decrypted password, db name
    """
    key = b'SmfcCo5I4Z96-lcHDN2sEW--ihLeZEwgVtg-to9Erfk='
    fernet = Fernet(key)
    encMessage = b'gAAAAABkRD6dXxIyHPBrKTQrHkiENYuDHL0KRqgIEpwqyQGkx2g4WVVfoOTrxZmHNqqRrAqY-Gs2P0_a5ZQY4-wuPPntmf23vw=='
    decMessage = fernet.decrypt(encMessage).decode()
    return 'localhost', 'root', decMessage, 'python_flask'


def get_db_reference() -> PooledMySQLConnection | MySQLConnection | CMySQLConnection | None:
    """
    fetches the reference for the local database
    :rtype: PooledMySQLConnection | MySQLConnection | CMySQLConnection | None
    :return: local database reference
    """
    dbHost, dbUser, dbPassword, dbName = get_db_credentials()
    try:
        return mysql.connector.connect(
            host=dbHost,
            user=dbUser,
            password=dbPassword,
            database=dbName
        )
    except Exception as e:
        print("Exception occurred while trying to establish database connection:", str(e))
        return None


def sync_db_with_csv(filename: str, storeStatus=None, menuHours=None, bqResults=None):
    """
    syncs the local database with the passed csv file
    :param filename: csv file name
    :param storeStatus: true if store status csv is passed
    :param menuHours: true if menu hours csv is passed
    :param bqResults: true if bq-results csv is passed
    :return: doesn't return anything, only to be used for syncing the db
    """
    print(f"DB UPDATE STARTED FOR {'store status' if storeStatus else ('menuHours' if menuHours else 'bqResults')}")
    myDb = get_db_reference()
    myDbCursor = myDb.cursor()
    start_time = time.time()
    with open(filename, 'r') as csvFile:
        reader = csv.DictReader(csvFile)
        batch_size = 100000
        if storeStatus:
            try:
                execute_db_sync_queries(batch_size, reader, myDbCursor, 'store_status', 'store_id', 'timestamp_utc',
                                     'status')
            except Exception as e:
                print("Exception occurred while trying to execute method 'execute_custom_query':", str(e))
        elif menuHours:
            try:
                execute_db_sync_queries(batch_size, reader, myDbCursor, 'menu_hours', 'store_id', 'day',
                                     'start_time_local',
                                     'end_time_local')
            except Exception as e:
                print("Exception occurred while trying to execute method 'execute_custom_query':", str(e))
        elif bqResults:
            try:
                execute_db_sync_queries(batch_size, reader, myDbCursor, 'store_timezone', 'store_id', 'timezone_str')
            except Exception as e:
                print("Exception occurred while trying to execute method 'execute_custom_query':", str(e))
        myDb.commit()
        myDbCursor.close()
        myDb.close()
        end_time = time.time()
        print(
            f"DB UPDATE FINISHED FOR {'store status' if storeStatus else ('menuHours' if menuHours else 'bqResults')}! Time taken:",
            end_time - start_time, "seconds")


def execute_db_sync_queries(batch_size: int, reader: object, db_cursor: MySQLCursor, dbName: str, *keys: object):
    """
    A re-usable method which executes query for deleting & inserting data to the database
    :param batch_size: max number of rows to be inserted to db at a time
    :param reader: csv file reader reference
    :param db_cursor: database reference/pointer
    :param dbName: database name where the operations are to be performed
    :param keys: keys/column names of the database where the operations are to be performed
    :return: nothing, only performs operations to the passed/relevant database
    """
    db_cursor.execute(f'DELETE FROM {dbName}')
    db_cursor.execute(f'ALTER TABLE {dbName} AUTO_INCREMENT = 1')
    values_list = []
    for row in reader:
        currRow = []
        for key in keys:
            currRow.append(row[key] if key != 'timestamp_utc' else row[key].rstrip(' UTC'))
        values_list.append(tuple(currRow))
        if len(values_list) >= batch_size:
            db_cursor.executemany(
                f'INSERT INTO {dbName} ({", ".join(keys)}) VALUES ({", ".join(["%s" for _ in keys])})', values_list)
            values_list = []
    if values_list:
        db_cursor.executemany(f'INSERT INTO {dbName} ({", ".join(keys)}) VALUES ({", ".join(["%s" for _ in keys])})',
                              values_list)


def execute_any_query(query):
    """
    executes any query to the db that is shared to the method
    :param query: query that needs to be executed
    :return: result of the query
    """
    db_reference = get_db_reference()
    dbCursor = db_reference.cursor()
    dbCursor.execute(query)
    results = dbCursor.fetchall()
    db_reference.commit()
    dbCursor.close()
    db_reference.close()
    return results if results else None


def execute_any_query_with_values(query, values):
    """
    executes any query to the db (along with values) that is shared to the method
    :param query: query that needs to be executed
    :param values: values that need to be put in the query
    :return: result of the query
    """
    db_reference = get_db_reference()
    dbCursor = db_reference.cursor()
    dbCursor.execute(query, values)
    results = dbCursor.fetchall()
    db_reference.commit()
    dbCursor.close()
    db_reference.close()
    return results if results else None


def sync_complete_store_db():
    """
    syncs the local database with all the given csv files
    :return: nothing, only syncs the local database with all the given csv files
    """
    print('DB_SERVICE RUNNING!')
    start_time = time.time()
    thread1 = threading.Thread(target=sync_db_with_csv, args=('Menu hours.csv', False, True, False))
    thread2 = threading.Thread(target=sync_db_with_csv,
                               args=('bq-results-20230125-202210-1674678181880.csv', False, False, True))
    thread3 = threading.Thread(target=sync_db_with_csv, args=('store status.csv', True, False, False))
    thread1.start()
    thread2.start()
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()
    end_time = time.time()
    print("Total time taken to update the database:", end_time - start_time, "seconds")


if __name__ == '__main__':
    sync_complete_store_db()
