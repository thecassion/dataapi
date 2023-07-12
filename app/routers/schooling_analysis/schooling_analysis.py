from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response
from .schema import SchoolingPositif

from .schooling_repo import processing_schooling_case, processing_schooling_case_stack, aggregate_cases_by_commune, aggregate_cases_add_departement
from json import dumps

router = APIRouter(
    prefix="/ovc_schooling",
    tags=["OVC_SCHOOLING"]
)


@router.get("/schooling_analysis", response_model=SchoolingPositif)
def read_schooling(year: str = "2022-2023", start_date: str = "2022-10-01", end_date: str = "2023-09-30"):
    try:
        merged_data = processing_schooling_case(year=year)
        payload = [
            merged_data['glob'],
            aggregate_cases_add_departement(merged_data['data'])
        ]
        # payload = aggregate_cases_add_departement(merged_data['data'])
        payload = dumps(payload).encode('utf-8')
        return Response(media_type="application/json", content=payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schooling_analysis/stack", response_model=SchoolingPositif)
def read_schooling_stack(year: str = "2022-2023", start_date: str = "2022-10-01", end_date: str = "2023-09-30"):
    try:
        merged_data = processing_schooling_case_stack(year=year)
        payload = [
            merged_data['glob'],
            aggregate_cases_add_departement(merged_data['data'])
        ]
        # payload = aggregate_cases_add_departement(merged_data['data'])
        payload = dumps(payload).encode('utf-8')
        return Response(media_type="application/json", content=payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
