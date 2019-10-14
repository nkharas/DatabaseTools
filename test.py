# Author : Nick Kharas

""" Testing the libraries
The test file is a 10000 row sample of the NYC green cab dataset for January 2019 (https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2019-01.csv)
"""

from MSSQLDatabaseTools import MSSQLDatabaseTools
import pytest
import pandas as pd
from ExceptionClasses import InputError
from database_credentials import db_credentials

server = db_credentials['server']
database = db_credentials['database']
uid = db_credentials['uid']
pwd = db_credentials['pwd']

# Test if Windows authentication is not used when uid and pwd are provided


def test_authentication():
    try:
        dbconn = MSSQLDatabaseTools(server, database, uid, pwd, True)
    except InputError as ie:
        print('Test passed')

# BCP data into table


def bcp_data():
    dbconn = MSSQLDatabaseTools(server, database, uid, pwd, False)
    dbconn.bcp_from_file(
        'test_data\\green_tripdata_2019-01_subset.csv', 'green_cab_trips', delimiter='|')

# Test full download of data


def full_download():
    dbconn = MSSQLDatabaseTools(server, database, uid, pwd, False)
    df = dbconn.download_from_table('green_cab_trips')
    assert df.shape[0] == 10000

    df2 = pd.read_csv('green_cab_trips.csv')
    assert df2.shape[0] == 10000

# Test if download with filtering works


def filtered_download():
    dbconn = MSSQLDatabaseTools(server, database, uid, pwd, False)
    df = dbconn.download_from_table(
        'green_cab_trips', where_clause='VendorID = 2')
    assert df.shape[0] == 8507

    df2 = pd.read_csv('green_cab_trips.csv')
    assert df2.shape[0] == 8507

# Output file has a name


def output_file_download():
    dbconn = MSSQLDatabaseTools(server, database, uid, pwd, False)
    df = dbconn.download_from_table('green_cab_trips', file_name='output_file')

    df = pd.read_csv('output_file.csv')
    assert df.shape[0] == 10000

# BCP append data into table


def bcp_data_append():
    dbconn = MSSQLDatabaseTools(server, database, uid, pwd, False)
    dbconn.bcp_from_file('test_data\\green_tripdata_2019-01_subset.csv',
                         'green_cab_trips', delimiter='|', drop_if_exist=False)

    df = dbconn.download_from_table('green_cab_trips')
    assert df.shape[0] == 20000

    df2 = pd.read_csv('green_cab_trips.csv')
    assert df2.shape[0] == 20000


test_authentication()
print('Test 1 passed')

bcp_data()
print('Function to BCP data ran')

full_download()
print('Test 2 passed')

filtered_download()
print('Test 3 passed')

output_file_download()
print('Test 4 passed')

bcp_data_append()
print('Test 5 passed')
