import os
import pymysql

from sqlalchemy import create_engine

def engine():
    print(os.environ)
    try:
        engine = create_engine('mysql+pymysql://'+os.environ["MYSQL_USERNAME"]+':'+os.environ["MYSQL_PASSWORD"]+'@'+os.environ['MYSQL_HOST']+':'+os.environ['MYSQL_PORT']+'/'+os.environ["MYSQL_DATABASE"]+'?charset=UTF8MB4', echo=True, encoding='utf8')
        return engine
    except Exception as e:
        raise Exception("Could not connect to database",e)