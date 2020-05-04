import psycopg2

conn = psycopg2.connect(dbname='[DB_NAME]', user='user_name',
        password='[PASSWORD]', host='localhost')
cur = conn.cursor()

list = ['fact.table_1', 'fact.table_2', 'fact.table_3', 'dim.schedule',
        'dim.schedule_category']


for table in list:
    sql = f"COPY {table} TO STDOUT WITH CSV DELIMITER ','"
    with open(f"C:\\Users\\[YOUR_PATH]\\{table}.csv", "w") as file:
        cur.copy_expert(sql, file)

cur.close()
