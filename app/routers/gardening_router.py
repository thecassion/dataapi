from fastapi import (
    APIRouter
)

from ..dataanalysis import (
    Gardening 
)


router = APIRouter(
    prefix="/gardening",
    tags=["Gardening"]
)

@router.get("/sync")
def sync_gardening():
    forms = Gardening().get_registration_forms()
    return forms
    return {"message":"Gardening sync successful"}

