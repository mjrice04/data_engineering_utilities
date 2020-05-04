import sqlalchemy

def connect(user, pw, db, host='localhost', port=5342):
    """
    Returns sqlalchemy connection and metadata
    Params:
    user: Postgres Username
    pw : password
    db : database
    host: place server if not localhost
    port: place port if not 5342
    """
    url = 'postgresql://{}:{}@{}/{}'
    url = url.format(user, pw, host, db)
    engine = sqlalchemy.create_engine(url, client_encoding='utf8')


    return engine
