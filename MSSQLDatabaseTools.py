# Author : Nick Kharas

import pyodbc as db
import pandas as pd
import bcpy
import sys
import gzip
import zipfile
import io

from ExceptionClasses import InputError
from DatabaseTools import DatabaseTools


class MSSQLDatabaseTools(DatabaseTools):
    """Specific implementation for Microsoft SQL Server
    """

    def __init__(self, server, database, uid='', pwd='', driver='SQL Server Native Client 11.0', trusted_connection=True):
        """Overriding constructor with specific database driver details. Default behavior is to support Windows authentication.
        :param server: Name of the database server.
        :param database: Name of the database.
        :param uid: User ID
        :param pwd: Password.
        :param trusted_connection: True for Windows Authentication, false for SQL Server Authentication.
        """
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.trusted_connection = trusted_connection
        self.driver = driver
        self.default_schema = 'dbo'
        self.table_metadata = 'sys.tables'

        if (len(self.uid) > 0 or len(self.pwd) > 0) and self.trusted_connection:
            raise InputError(
                'ERROR - AUTHENTICATION: Cannot use Windows Authentication if user name and password is provided')

    def connect_to_db(self):
        """Override DB Connection to allow for Windows Authentication
        """
        if self.trusted_connection:
            cnxn = db.connect(r'Driver={' + self.driver + '};Server=' + self.server +
                              ';Database=' + self.database + ';Trusted_Connection=yes;')
        else:
            cnxn = db.connect(r'Driver={' + self.driver + '};Server=' + self.server +
                              ';Database=' + self.database + ';UID=' + self.uid + ';PWD=' + self.pwd)

        return cnxn

    def bcp_from_df(self, df, table_name, drop_if_exist=True, batch_size=10000):
        """BCP data from a pandas dataframe into a SQL table. You will need to install Microsoft BCP in your system.
        :param df: Pandas dataframe containing the data to load
        :param table_name: Name of the table that the data should be loaded into
        :param drop_if_exist: Provides an option to drop the table if it already exists in the database.
                              Default behavior is to drop and recreate the table.
        :param batch_size: Numbwer of rows that get BCP inserted into the table in one batch.
        """
        # First, remove spaces in column names, otherwise BCP will fail
        colnames = list(df.columns)
        colnames_new = [cn.replace(' ', '_') for cn in colnames]
        coldict = dict()
        for (cn, cnn) in zip(colnames, colnames_new):
            coldict[cn] = cnn
        df = df.rename(columns=coldict)

        # Check what needs to be done if the table already exists
        if drop_if_exist:
            self.drop_table_if_exists(table_name)

        # MSSQL config
        if self.trusted_connection:
            sql_config = {'server': self.server, 'database': self.database}
        else:
            sql_config = {'server': self.server, 'database': self.database,
                          'username': self.uid, 'password': self.pwd}
        bdf = bcpy.DataFrame(df)
        sql_table = bcpy.SqlTable(sql_config, table=table_name)

        # If the table does not exist, create it, else directly append data into it.
        bdf.to_sql(sql_table, use_existing_sql_table=self.is_table_exists(
            table_name), batch_size=batch_size)

    def bcp_from_file(self, file_name, table_name, delimiter=',', drop_if_exist=True, batch_size=10000):
        """BCP data from a flat file into a SQL table. You will need to install Microsoft BCP in your system.
        :param file_name: Fully qualified name of the file containing the data to load
        :param table_name: Name of the table that the data should be loaded into
        :param delimiter: Row delimiter. Default is ,
        :param drop_if_exist: Provides an option to drop the table if it already exists in the database.
                              Default behavior is to drop and recreate the table.
        :param batch_size: Numbwer of rows that get BCP inserted into the table in one batch.
        """

        df = pd.read_csv(file_name, delimiter=delimiter)
        self.bcp_from_df(df=df, table_name=table_name,
                         drop_if_exist=drop_if_exist, batch_size=batch_size)

    def bcp_from_gzip(self, file_name, table_name, delimiter=',', drop_if_exist=True, batch_size=10000):
        """BCP data from a gzip compressed file into a SQL table. You will need to install Microsoft BCP in your system.
        :param file_name: Fully qualified name of the gzip file containing the data to load
        :param table_name: Name of the table that the data should be loaded into
        :param delimiter: Row delimiter. Default is ,
        :param drop_if_exist: Provides an option to drop the table if it already exists in the database.
                              Default behavior is to drop and recreate the table.
        :param batch_size: Numbwer of rows that get BCP inserted into the table in one batch.
        """

        zipped = gzip.open(file_name, 'rb')
        data = zipped.read()
        data_decode = io.StringIO(data.decode('utf_8'))
        df = pd.read_csv(data_decode, dtype=str, delimiter=delimiter)
        self.bcp_from_df(
            df, table_name, drop_if_exist=drop_if_exist, batch_size=batch_size)

    def bcp_from_zip(self, file_name, table_name, delimiter=',', drop_if_exist=True, batch_size=10000):
        """BCP data from a zip compressed file into a SQL table. You will need to install Microsoft BCP in your system.
        :param file_name: Fully qualified name of the zip file containing the data to load
        :param table_name: Name of the table that the data should be loaded into
        :param delimiter: Row delimiter. Default is ,
        :param drop_if_exist: Provides an option to drop the table if it already exists in the database.
                              Default behavior is to drop and recreate the table.
        :param batch_size: Number of rows that get BCP inserted into the table in one batch.
        """

        file_zip = zipfile.ZipFile(file_name)
        for unzipped_name in file_zip.namelist():
            data = file_zip.read(unzipped_name)
            data_decode = io.StringIO(data.decode('utf_8'))
            df = pd.read_csv(data_decode, dtype=str, delimiter=delimiter)
        self.bcp_from_df(
            df, table_name, drop_if_exist=drop_if_exist, batch_size=batch_size)
