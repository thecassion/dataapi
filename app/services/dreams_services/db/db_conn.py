import pymysql
from sqlalchemy import create_engine, text
from pandas import read_sql_query

from ....core import settings
from ..utils import (
    QUERY_MASTER,
    QUERY_PERIOD
)

engine = create_engine(
    f"mysql+pymysql://{settings.mysql_username}:{settings.mysql_password}@{settings.mysql_host}/{settings.mysql_database}")


agyw_served_period = read_sql_query(text(QUERY_PERIOD), engine.connect(), parse_dates=True)
agyw_served = read_sql_query(text(QUERY_MASTER), engine.connect(), parse_dates=True)

# close the pool of connection
engine.dispose()
