import pymysql
from sqlalchemy import  text
from pandas import read_sql_query
from dateutil.relativedelta import relativedelta


from .eid_query import EID
from .eid_summary_processing import Eid
from .eid_status import StatusEid

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




def summary_eid_total(engine,year=None, quarter=None, network=None):
    eid = data_processing(engine)
    result_eid = Eid.pmtctEID_heiPos_ON_ARV(eid,year,quarter,network)
    title_eid = Eid.get_filter_title(eid)
    return {
        "year_title": title_eid["year_title"],
        "quarter_title": title_eid["quarter_title"],
        "network_title": title_eid["network_title"],
        "summary": result_eid.to_dict('records')
        
    }



def testing_eid_total(engine,year=None, office=None, hospital=None):
    eid = data_processing(engine)
    result_eid = StatusEid.testing_status(eid,year,office,hospital)
    #title_eid = StatusEid.get_filter_title(eid)
    """ return {
        "titles": title_eid,
        "columns":{
            "Year": result_eid.Year.tolist(),
            "Office": result_eid.Office.tolist(),
            "Hospital": result_eid.hospital.tolist(),
            "Result": result_eid.Result.tolist(),
            "Quarter": result_eid.Quarter.tolist(),
            "tranche_age": result_eid.tranche_age.tolist(),
            "Liaison_mere": result_eid.Liaison_mere.tolist(),
        }
        
    } """
    return {
            "Year": result_eid.Year.tolist(),
            "Office": result_eid.Office.tolist(),
            "Hospital": result_eid.hospital.tolist(),
            "Result": result_eid.Result.tolist(),
            "Quarter": result_eid.Quarter.tolist(),
            "tranche_age": result_eid.tranche_age.tolist(),
            "Liaison_mere": result_eid.Liaison_mere.tolist(),
        
    }

