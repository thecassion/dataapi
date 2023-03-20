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

def data_processing(engine):
    agyw_served_period = read_sql_query(text(QUERY_PERIOD), engine.connect(), parse_dates=True)
    agyw_served = read_sql_query(text(QUERY_MASTER), engine.connect(), parse_dates=True)
    engine.dispose()
    AGYW_ACTIF = AGYW_Analysis(agyw_served,agyw_served_period).data_actif_served()
    datim = AgywPrev(data=AGYW_ACTIF)
    return datim

def run_agywprevI(engine):
    datim  = data_processing(engine)
    return [
        {
            "who_am_i": datim.who_am_i,
            "name": "AGYW_PREV",
            "total": datim.total_datim_general
        },
        {
            "table_name": "Table I",
            "description": datim.datim_titleI(),
            'total': datim.total_datimI,
            "rows": datim.datim_agyw_prevI().to_dict('records'),
            "rows_title": datim.datim_agyw_prevI().columns.to_list()
        }
    ]


def run_agywprevII(engine):
    datim  = data_processing(engine)
    return [
        {
            "who_am_i": datim.who_am_i,
            "name": "AGYW_PREV",
            "total": datim.total_datim_general
        },
        {
            "table_name": "Table II",
            "description": datim.datim_titleII(),
            'total': datim.total_datimII,
            "rows": datim.datim_agyw_prevII().to_dict('records'),
            "rows_title": datim.datim_agyw_prevII().columns.to_list()
        }
    ]

def run_agywprevIII(engine):
    datim  = data_processing(engine)
    return [
        {
            "who_am_i": datim.who_am_i,
            "name": "AGYW_PREV",
            "total": datim.total_datim_general
        },
        {
            "table_name": "Table III",
            "description": datim.datim_titleIII(),
            'total': datim.total_datimIII,
            "rows": datim.datim_agyw_prevIII().to_dict('records'),
            "rows_title": datim.datim_agyw_prevIII().columns.to_list()
        }
    ]


def run_agywprevIV(engine):
    datim  = data_processing(engine)
    return [
        {
            "who_am_i": datim.who_am_i,
            "name": "AGYW_PREV",
            "total": datim.total_datim_general
        },
        {
            "table_name": "Table IV",
            "description": datim.datim_titleIV(),
            'total': datim.total_datimIV,
            "rows": datim.datim_agyw_prevIV().to_dict('records'),
            "rows_title": datim.datim_agyw_prevIV().columns.to_list()
        }
    ]

def run_vital_info(engine):
    datim  = data_processing(engine)
    return [
        {
            "rows_title": [
                datim.datim_vital_info().to_dict('tight')['columns'][0],
                datim.datim_vital_info().to_dict('tight')['data'][0][0],
                datim.datim_vital_info().to_dict('tight')['data'][1][0]
            ],
            "value_row1":datim.datim_vital_info().to_dict('tight')['columns'][1],
            "value_row2": datim.datim_vital_info().to_dict('tight')['data'][0][1],
            "value_row3": datim.datim_vital_info().to_dict('tight')['data'][1][1]
        }
    ]
