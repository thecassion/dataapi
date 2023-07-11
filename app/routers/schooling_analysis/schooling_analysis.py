from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from .schema import SchoolingPositif

from .schooling_repo import merged_data, aggregate_cases_by_commune

router = APIRouter(
    prefix="/ovc_schooling",
    tags=["OVC_SCHOOLING"]
)


@router.get("/schooling_analysis", response_model=SchoolingPositif)
def read_schooling():
    try:
        payload = [
            merged_data['glob'],
            *aggregate_cases_by_commune(merged_data['data'])
        ]
        # count_payload = merged_data
        return JSONResponse(content=payload, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
