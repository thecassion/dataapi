from fastapi import (
    APIRouter,
    HTTPException
)

from .db import PtmeOvc as ptme_ovc
import io
import pandas as pd
import datetime
from starlette.responses import StreamingResponse



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

@router.get("/semester/xlsx/")
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



@router.get("/test")
def test():
    return {"message":"test"}