
import logging
import os
import pandas as pd
import sqlalchemy
import pyodbc
import urllib



def create_mssql_connection(server,database):
    """
    Create a SQL alchemy engine
    
    Args:
        :database = name of the database to connect to
    Returns:
        SQLAlchemy database connection
    """
    
    computername = os.getenv('COMPUTERNAME')

    
    quoted = urllib.parse.quote_plus(
                "DRIVER={ODBC Driver 13 for SQL Server};"
                "SERVER=%(server)s;DATABASE=%(database)s;"
                "Trusted_Connection=yes"
                % {"database": database})
    conn_string = "mssql+pyodbc:///?odbc_connect=%s" % quoted
    

    return sqlalchemy.create_engine(conn_string)



def limit_query(query, no_rows):
    """
    Appends a limit to any user query so we do not return too many rows

    Args:
        :query = sql query the user provides
        :no_rows = the number of rows to limit

    Returns:
        New Query string with limit attached
    """
    rows_limit = str(no_rows)
    limit = "SET ROWCOUNT %s;" % rows_limit
    new_query = limit + '\n' + query
    return(new_query)

def stored_proc_no_return(database,query):
    """
    Purpose: Executes a Stored Proc that does not return a value

    Args:
        database: Database to target
        query: Query
    """
    engine = create_mssql_connection(database)
    cursor = engine.raw_connection().cursor()
    cursor.execute(query)
    cursor.commit()




def sql_to_pandas(database, query):
    """
    Create a SQL alchemy engine and returns the results of a query in a pandas dataframe

    Args:
        :database = name of the database to connect t
        :query    = user query
    Returns:
        Dataframe of the results of a the query
    """
    #logger.debug('Getting data from sql using:  ' + query)

    engine = create_mssql_connection(database)
    
    cnxn = engine.connect().connection

    new_query = limit_query(query,1000000)
    
    ds = pd.read_sql(new_query, cnxn)
    cnxn.invalidate()
    cnxn.close()
    engine.dispose()
    return ds
