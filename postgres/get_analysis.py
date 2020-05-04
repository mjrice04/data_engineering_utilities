import psycopg2
import sys
import pandas as pd
from python_to_sql import connect
from datetime import datetime, timedelta
import os


def get_data(query): 
    engine = connect('books',os.environ['DB_PASSWD'],'bookdb')
    try:
        df = pd.read_sql(query,con=engine)
        return df
    except Exception as e:
        print(e)
        return e


def run_query(price):
    date = datetime.today().strftime('%Y-%m-%d')
    query =  '''
             select asin, 
             bookname, 
             author,  
             price 
             from kindlebooks where scrapedate = '%s' and price <  %s
             ''' % (date,price)
    df = get_data(query)
    return df
