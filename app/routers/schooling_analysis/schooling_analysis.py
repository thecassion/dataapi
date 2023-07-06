from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from .schema import SchoolingPositif
from .db import (
    collection,
    schooling_oev_collection,
    schooling_siblings_collection,
    schooling_cwv_collection,
    schooling_dreams_collection
)
from .schooling_repo import merged_data

router = APIRouter(
    prefix="/ovc_schooling",
    tags=["OVC_SCHOOLING"]
)


@router.get("/schooling_analysis", response_model=SchoolingPositif)
def read_schooling():
    try:
        count_payload = [merged_data]
        return JSONResponse(content=count_payload, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
