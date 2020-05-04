from sql_engine_setup import create_mssql_connection
import pandas as pd
import os
import sys


def write_to_sql(df,tablename, tableschema='src'):
    """
    Writes CSV to Raw Database in the 'schema' of the user's choice. Defaults to src
    """
    df.to_sql(name=tablename, schema=tableschema, index=False, if_exists='replace', con=engine) #connects to SQL
    print("%s successfully created" % (tablename))                                              #Prints out messafe


def load_ref_files():
    """
    Reads in Reference File and calls the write_to_sql function
    """
    Folder = '[FOLDER_NAME]'  #Name of Folder in the repo

    ListofRefFiles = os.listdir(Folder) #List of Reference files

    for file in ListofRefFiles: # Loop through all the files
        ext = file[-3:]         # Looking for the extension
        tablename = file[:-4]   # Getting the table name
        filepath = Folder + '/' + file #gathering the filepath
        if ext == 'txt': #if it is table delimited
            df = pd.read_csv(filepath, sep='\t', encoding='ISO-8859-1')
            write_to_sql(df,tablename)
        else:            #if the file is a csv
            df = pd.read_csv(filepath, encoding='ISO-8859-1')
            write_to_sql(df,tablename)

if __name__ == '__main__':
    server = sys.argv[1]
    database = sys.argv[2]
    print(server)
    print(database)
    engine = create_mssql_connection(server, database)
    load_ref_files()

        
