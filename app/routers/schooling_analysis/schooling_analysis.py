from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse, Response
from .schema import SchoolingPositif

from .schooling_repo import processing_schooling_case, aggregate_cases_add_departement
from json import dumps
from .schooling_repo_patient_logic import PtmeOev

router = APIRouter(
    prefix="/ovc_schooling",
    tags=["OVC_SCHOOLING"]
)


@router.get("/schooling_analysis", response_model=SchoolingPositif, status_code=status.HTTP_200_OK)
def read_schooling(year: str = "2022-2023", start_date: str = "2022-10-01", end_date: str = "2023-09-30"):
    try:
        merged_data = processing_schooling_case(
            year=year, start_date=start_date, end_date=end_date)
        payload = [
            merged_data['glob'],
            aggregate_cases_add_departement(merged_data['data'])
        ]
        # payload = aggregate_cases_add_departement(merged_data['data'])
        payload = dumps(payload).encode('utf-8')
        return Response(media_type="application/json", content=payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/ptmeOev_query/test")
def test(start_date: str = "2022-10-01", end_date: str = "2023-09-30"):
    payload = PtmeOev().get_ovc_by_period(start_date, end_date)
    payload = dumps(payload).encode('utf-8')
    return Response(media_type="application/json", content=payload)
    return payload


@router.get("/comparaison/test")
def test2(start_date: str = "2022-10-01", end_date: str = "2023-09-30"):
    payload = PtmeOev().compare_results(start_date, end_date)
    payload = dumps(payload).encode('utf-8')
    return Response(media_type="application/json", content=payload)
    return payload
