import psycopg2
import os

conn = psycopg2.connect(f"host=localhost dbname=bookdb
        user={os.environ['USER']} password='{os.environ['DB_PASSWD']}'")

cur = conn.cursor()

with open('data/sample.csv', 'r') as f:
    cur.copy_from(f, 'kindlebooks', sep='|', null="")


conn.commit()

