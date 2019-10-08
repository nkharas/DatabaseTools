# Author : Nick Kharas

#Importing libraries
import pyodbc as db
import pandas as pd
import sys

from ExceptionClasses import InputError


class DatabaseTools:
    """
    Tools that allow coders to connect to a relational database, and either download data into a pandas dataframe / flat file,
    or BCP data from a pandas dataframe / flat file into a table
    """

    def __init__(self, driver, server, database, uid, pwd, default_schema, table_metadata):
        """All database operations need basic credentials to connect to the database. We initialize the class with the database credentials.
        :param driver: Name of the database driver. All child classes for specific implementations will overwrite this parameter.
        :param server: Name of the database server.
        :param database: Name of the database.
        :param uid: User ID
        :param pwd: Password.
        :param default_schema: Default schema where tables belong.
        :param table_metadata: Name of the metadata table that has the names and details of other tables in the database.
        """
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd
        self.trusted_connection = trusted_connection
        self.driver = driver
        self.default_schema = default_schema
        self.table_metadata = table_metadata
    

    def connect_to_db(self):
        """Establish DB Connection to use in database operations
        """
        cnxn = db.connect(r'Driver={' + self.driver + '};Server=' + self.server + ';Database=' + self.database + ';UID=' + self.uid + ';PWD=' + self.pwd)
        return cnxn


    def is_table_exists(self, table_name):
        """Return True if the table already exists in the database, False otherwise
        :param table_name: Name of the table to check
        """

        #Setting the connection
        cnxn = cnxn = self.connect_to_db()
        cursor = cnxn.cursor()

        # Check if table exists, note that checking in sys.tables is very specific to MSSQL Server
        cursor.execute("select * from " + self.table_metadata + " where name = '" + table_name + "'")

        # If query retruns a result, return True, else return False
        output = cursor.fetchall()
        output_list = [list(i) for i in output]
        if len(output_list) > 0:
            return True
        else:
            return False


    def drop_table_if_exists(self, table_name):
        """Drop the table if it exists
        :param table_name: Name of the table to drop
        """

        if self.is_table_exists(table_name):
            #Setting the connection
            cnxn = self.connect_to_db()
            cursor = cnxn.cursor()

            # Check if table exists, note that checking in sys.tables is very specific to MSSQL Server
            cursor.execute('drop table ' + table_name)
            cursor.commit()
        
        
    def download_from_table(self, table_name, where_clause = '', file_name = '', schema = ''):
        """Download all data from a SQL table into a flat file and pandas dataframe
        :param table_name: Name of the table to select data and download from
        :param where_clause: Optional parameter. Where clause to filter the data.
        :param file_name: Optional parameter. Name of the output file. Default is to use the input table name as the output file name.
        :param schema: Name of the schema where table belongs
        """
        #Setting the connection
        cnxn = self.connect_to_db()
        cursor = cnxn.cursor()

        # Checking for optional parameters
        if len(where_clause) > 0:
            where_clause = ' where ' + where_clause

        if len(file_name) == 0:
            file_name = table_name

        if len(schema) == 0:
            schema = self.default_schema
        
        #Running the query
        cursor.execute('select * from ' + schema + '.' + table_name + ' ' + where_clause)

        #Fetching first object ( column name) from tuple returned by cursor.description
        output_cols = [i[0] for i in cursor.description]

        #Loading all the result rows
        output = cursor.fetchall()

        #Converting tuples to list
        output_list = [list(i) for i in output]

        #Creating dataframe
        df = pd.DataFrame(data=output_list,columns=output_cols)

        # Create CSV file
        df.to_csv(file_name + '.csv', index = False, index_label = False)
        return df






