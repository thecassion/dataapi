from fastapi import (
    APIRouter,
    HTTPException
)
from .vbg_groupes_members_cases import VBGGroupesMembersCase
from ...db.vbg import Vbg


router = APIRouter(
    prefix="/vbg",
    tags=["VBG"]
)

@router.get("/link/sync")
def sync_other_visit_ptme():
    return VBGGroupesMembersCase().get()

@router.get("/groupes/infos")
def get_groupes_infos():
    return Vbg().get_vbg_groupes_infos()

@router.get("/groupes/members/infos")
def get_groupes_members_infos():
    return Vbg().get_vbg_groupe_members_infos()