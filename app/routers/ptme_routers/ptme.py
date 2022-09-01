from fastapi import (
    APIRouter,
    HTTPException
)


router = APIRouter(
    prefix="/ptme",
    tags=["PTME"]
)

@router.get("/other_visit/sync")
def sync_other_visit_ptme():
    return {"Test":"Done"}
