from fastapi import (
    APIRouter,
    HTTPException
)

from .db import PtmeOvc as ptme_ovc
from .db.muso import Muso as muso_ovc
from .db.dreams import Dreams as dreams_ovc
import io
import pandas as pd
import datetime
from starlette.responses import StreamingResponse
from datetime import date



router = APIRouter(
    prefix="/ovc",
    tags=["OVC"]
)

@router.get("/semester")
def sync_other_visit_ptme(report_year_1: int, report_quarter_1: int, report_year_2: int, report_quarter_2: int,type_of_aggregation=None):
    return ptme_ovc().get_ovc_serv_semester(report_year_1, report_quarter_1, report_year_2, report_quarter_2, type_of_aggregation)

@router.get("/appel_ptme")
def appel_ptme(report_year,report_quarter, type_appel="APPELS_PTME"):
    return ptme_ovc().get_appel_ptme_from_mongo(report_year,report_quarter, type_appel)

@router.get("/semester/xlsx")
def ovc_semester(report_year_1: int, report_quarter_1: int, report_year_2: int, report_quarter_2: int,type_of_aggregation=None):
    json_dict =  ptme_ovc().get_ovc_serv_semester(report_year_1, report_quarter_1, report_year_2, report_quarter_2, type_of_aggregation)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        pd.DataFrame(json_dict).to_excel(writer, sheet_name="OVC_SERV_SEMESTER")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y-%m-%d")

    headers = {
        'Content-Disposition': 'attachment; filename=ovc_semester_'+date+'.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)

@router.get("/ptme/household")
def ptme_household(report_year,report_quarter):
    return ptme_ovc().get_ptme_household(report_year,report_quarter)

@router.get("/infant/household/")
def infant_household(report_year,report_quarter):
    return ptme_ovc().get_infant_household(report_year,report_quarter)


@router.get("/muso/semester/all")
def muso_all(report_year_start,report_quarter_start,type_of_aggregation="commune"):
    return muso_ovc().get_ovc_muso_all(report_year_start,report_quarter_start, type_of_aggregation)

@router.get("/muso/semester/all/xlsx")
def muso_all_xlsx(report_year_start,report_quarter_start, type_of_aggregation="commune"):
    json_dict = muso_ovc().get_ovc_muso_all(report_year_start,report_quarter_start,type_of_aggregation)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        pd.DataFrame(json_dict).to_excel(writer, sheet_name="OVC_MUSO_ALL")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y-%m-%d")

    headers = {
        'Content-Disposition': 'attachment; filename=ovc_muso_all_'+date+'.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)

@router.get("/muso/semester/carismemberless")
def muso_carismemberless(report_year_start,report_quarter_start, type_of_aggregation="commune"):
    return muso_ovc().get_ovc_muso_without_caris_member(report_year_start,report_quarter_start,type_of_aggregation)

@router.get("/muso/semester/carismemberless/xlsx")
def muso_carismemberless_xlsx(report_year_start,report_quarter_start, type_of_aggregation="commune"):
    json_dict = muso_ovc().get_ovc_muso_without_caris_member(report_year_start,report_quarter_start,type_of_aggregation)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        pd.DataFrame(json_dict).to_excel(writer, sheet_name="OVC_MUSO_CARIS_MEMBERLESS")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y-%m-%d")

    headers = {
        'Content-Disposition': 'attachment; filename=ovc_muso_caris_memberless_'+date+'.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)

@router.get("/dreams")
def dreams(start_date:date, end_date:date, type_of_aggregation="commune"):
    return dreams_ovc().get_ovc_dreams_by_period(start_date, end_date, type_of_aggregation)
