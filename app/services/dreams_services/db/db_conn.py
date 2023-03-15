import pymysql
from sqlalchemy import create_engine
from pandas import read_sql_query

from ....core import settings
from ..utils import (
    QUERY_MASTER,
    QUERY_PERIOD
)

engine = create_engine(
    f"mysql+pymysql://{settings.mysql_username}:{settings.mysql_password}@{settings.mysql_host}/{settings.mysql_database}")


agyw_served_period = read_sql_query(QUERY_PERIOD, engine, parse_dates=True)
agyw_served = read_sql_query(QUERY_MASTER, engine, parse_dates=True)

# close the pool of connection
engine.dispose()
