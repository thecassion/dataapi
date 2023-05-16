from fastapi import (
    APIRouter,
    HTTPException
)

from .db import PtmeOvc as ptme_ovc



router = APIRouter(
    prefix="/ovc",
    tags=["OVC"]
)

@router.get("/semester/")
def sync_other_visit_ptme(report_year_1: int, report_quarter_1: int, report_year_2: int, report_quarter_2: int):
    return ptme_ovc().get_ovc_serv_semester(report_year_1, report_quarter_1, report_year_2, report_quarter_2)