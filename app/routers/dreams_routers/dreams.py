from fastapi import (
    APIRouter,
    status,
    HTTPException
)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from json import dumps
from numpyencoder import NumpyEncoder

from typing import List

from ...services.services_dreams import (
    run_agywprevI,
    run_agywprevII,
    run_agywprevIII,
    run_agywprevIV,
    run_vital_info,
    run_datim
)

from ...services.services_enrolement_dreams.model import ScreenedVsEligible, EligibleVsToBeServed, ServedPerTrimester
from ...services.services_enrolement_dreams.data import EnrolementAnalysis

from ...core import settings

router = APIRouter(
    prefix='/dreams',
    tags=['DREAMS']
)


@router.get(
    '/datim',
    response_description=settings.DATIM_DESCRIPTION,
    summary=settings.DATIM_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def datim():
    DATIM_SCHEMA = run_datim()
    if DATIM_SCHEMA:
        json_datim = dumps(DATIM_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


@router.get(
    '/vital_info',
    response_description=settings.VITAL_DESCRIPTION,
    summary=settings.VITAL_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def vital_info():
    VITAL_SCHEMA = run_vital_info()
    if VITAL_SCHEMA:
        json_datim = dumps(VITAL_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


@router.get(
    '/screenedVSelgible',
    response_description="get the screened and the eligible in dreams",
    summary="get the screened and eligible in dreams",
    status_code=status.HTTP_200_OK,
    response_model=List[ScreenedVsEligible]
)
async def screened_vs_eligible():
    data = EnrolementAnalysis.screened_versus_eligible()
    if data:
        return [data]
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


@router.get(
    '/eligibleVsToBeServed',
    response_description="get the to be served and the eligible in dreams",
    summary="get the to be served and eligible in dreams",
    status_code=status.HTTP_200_OK,
    response_model=List[EligibleVsToBeServed]
)
async def eligible_vs_to_be_served():
    data = EnrolementAnalysis.eligible_to_be_served()
    if data:
        return [data]
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


@router.get(
    '/servedPerTrimester',
    response_description="get the unserved per trimester",
    summary="get the UNSERVED per trimester",
    status_code=status.HTTP_200_OK,
    response_model=List[ServedPerTrimester]
)
async def served_per_trismester():
    data = EnrolementAnalysis.to_be_served_per_trimester()
    if data:
        return [data]
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")
