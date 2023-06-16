from fastapi import (
    APIRouter,
    HTTPException
)


router = APIRouter(
    prefix="/schooling",
    tags=["SCHOOLING"]
)

@router.get("/schooling_analysis/count")
def schooling_analysis():
    return {"Test":"schooling Analysis"}