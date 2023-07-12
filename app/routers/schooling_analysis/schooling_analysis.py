from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response
from .schema import SchoolingPositif

from .schooling_repo import merged_data, aggregate_cases_by_commune, aggregate_cases_add_departement
from json import dumps

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


@router.get("/schooling_analysis/departement", response_model=SchoolingPositif)
def read_schooling_dept():
    try:
        payload = [
            merged_data['glob'],
            aggregate_cases_add_departement(merged_data['data'])
        ]
        # payload = aggregate_cases_add_departement(merged_data['data'])
        payload = dumps(payload).encode('utf-8')
        return Response(media_type="application/json", content=payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
