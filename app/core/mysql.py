from datetime import date
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine

from .settings import settings

def engine():
    try:

        # Connect to the database
        connection = pymysql.connect(host=settings.mysql_host,
                                    user=settings.mysql_username,
                                    password=settings.mysql_password,
                                    database=settings.mysql_database,
                                    port= settings.mysql_port,
                                    cursorclass=pymysql.cursors.DictCursor
                                    )
        return connection
        # engine = create_engine('mysql+pymysql://'+os.environ["MYSQL_USERNAME"]+':'+os.environ["MYSQL_PASSWORD"]+'@'+os.environ['MYSQL_HOST']+':'+os.environ['MYSQL_PORT']+'/'+os.environ["MYSQL_DATABASE"]+'?charset=UTF8MB4', echo=True, encoding='utf8')
        return engine
    except Exception as e:
        raise Exception("Could not connect to database",e)

def sql_achemy_engine():
    try:
        engine = create_engine(f"mysql+pymysql://{settings.mysql_username}:{settings.mysql_password}@{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}?charset=UTF8MB4", echo=True, encoding='utf8')
        return engine
    except Exception as e:
        raise Exception("Could not connect to database",e)
