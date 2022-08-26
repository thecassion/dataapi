from datetime import date
import os
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine

def engine():
    try:

        # Connect to the database
        connection = pymysql.connect(host=os.environ["MYSQL_HOST"],
                                    user=os.environ["MYSQL_USERNAME"],
                                    password=os.environ["MYSQL_PASSWORD"],
                                    database=os.environ["MYSQL_DATABASE"],
                                    port= int(os.environ["MYSQL_PORT"]),
                                    cursorclass=pymysql.cursors.DictCursor
                                    )
        return connection
        # engine = create_engine('mysql+pymysql://'+os.environ["MYSQL_USERNAME"]+':'+os.environ["MYSQL_PASSWORD"]+'@'+os.environ['MYSQL_HOST']+':'+os.environ['MYSQL_PORT']+'/'+os.environ["MYSQL_DATABASE"]+'?charset=UTF8MB4', echo=True, encoding='utf8')
        return engine
    except Exception as e:
        raise Exception("Could not connect to database",e)

def sql_achemy_engine():
    try:
        engine = create_engine('mysql+pymysql://'+os.environ["MYSQL_USERNAME"]+':'+os.environ["MYSQL_PASSWORD"]+'@'+os.environ['MYSQL_HOST']+':'+os.environ['MYSQL_PORT']+'/'+os.environ["MYSQL_DATABASE"]+'?charset=UTF8MB4', echo=True, encoding='utf8')
        return engine
    except Exception as e:
        raise Exception("Could not connect to database",e)