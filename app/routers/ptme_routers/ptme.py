from fastapi import (
    APIRouter,
    HTTPException
)

from ...db.pmtct import Pmtct

router = APIRouter(
    prefix="/ptme",
    tags=["PTME"]
)

@router.get("/other_visit/sync")
def sync_other_visit_ptme():
    return {"Test":"Done"}

@router.get("/club/infos")
def get_club_infos():
    return Pmtct().get_club_6_month_active_infos()

@router.get("/club/pmtct/infos")
def get_club_pmtct_infos():
    return Pmtct().get_number_pmtct_active_in_club()