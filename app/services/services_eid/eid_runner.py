import pymysql
from sqlalchemy import  text
from pandas import read_sql_query
from dateutil.relativedelta import relativedelta


from .eid_query import EID
from .eid_summary_processing import pmtctEID_heiPos_ON_ARV

from ...core import settings

def data_processing(engine):
    eid = read_sql_query(text(EID), engine.connect(), parse_dates=True)
    engine.dispose()
    eid.rename(columns = {'Site_or_lab_code' : 'CODE LABO'}, inplace = True)
    eid.tranche_age.replace({'':'--to.calculate--'}, inplace=True)
    eid['calculated_ageDiff'] = eid.apply(lambda eid: relativedelta(eid.date_blood_taken, eid.date_of_birth).months,axis=1)
    eid['calculated_ageDiff'] = eid['calculated_ageDiff'].abs()
    eid.loc[(eid.calculated_ageDiff>=0) & (eid.calculated_ageDiff<=2),"tranche_age"] = "0_2"
    eid.loc[(eid.calculated_ageDiff>2) & (eid.calculated_ageDiff<=12),"tranche_age"] = "2_12"
    return eid




def summary_eid_total(engine,year=None):
    eid = data_processing(engine)
    result_eid = pmtctEID_heiPos_ON_ARV(eid,year)
    return result_eid.to_dict('records')
