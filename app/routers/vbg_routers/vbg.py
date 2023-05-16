from fastapi import (
    APIRouter,
    HTTPException
)
from .vbg_groupes_members_cases import VBGGroupesMembersCase


router = APIRouter(
    prefix="/vbg",
    tags=["VBG"]
)

@router.get("/link/sync")
def sync_other_visit_ptme():
    return VBGGroupesMembersCase().get()