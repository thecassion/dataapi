import pymysql
from sqlalchemy import  text
from pandas import read_sql_query

from .analysis import AGYW_Analysis
from .agyw import AgywPrev

from .utils import (
    QUERY_MASTER,
    QUERY_PERIOD
)

from ...core import settings

def run_datim(engine):
    agyw_served_period = read_sql_query(text(QUERY_PERIOD), engine.connect(), parse_dates=True)
    agyw_served = read_sql_query(text(QUERY_MASTER), engine.connect(), parse_dates=True)
    engine.dispose()
    AGYW_ACTIF = AGYW_Analysis(agyw_served,agyw_served_period).data_actif_served()
    datim = AgywPrev(data=AGYW_ACTIF)
    return {
        "who_am_i": datim.who_am_i,
        "total_datim": int(datim.total_datim_general),
        "titleI": datim.datim_titleI(),
        "tableI": datim.datim_agyw_prevI().to_dict('split'),
        'total_datimI': int(datim.total_datimI),
        "titleII": datim.datim_titleII(),
        "tableII": datim.datim_agyw_prevII().to_dict('split'),
        'total_datimII': int(datim.total_datimII),
        "titleIII": datim.datim_titleIII(),
        "tableIII": datim.datim_agyw_prevIII().to_dict('split'),
        'total_datimIII': int(datim.total_datimIII),
        "titleIV": datim.datim_titleIV(),
        "tableIV": datim.datim_agyw_prevIV().to_dict('split'),
        'total_datimIV': int(datim.total_datimIV)
    }
