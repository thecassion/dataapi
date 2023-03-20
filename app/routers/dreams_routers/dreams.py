from fastapi import (
    APIRouter,
    status,
    HTTPException
)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from json import dumps
from numpyencoder import NumpyEncoder



from ...services.services_dreams import (
    run_agywprevI,
    run_agywprevII,
    run_agywprevIII,
    run_agywprevIV,
    run_vital_info
)

from ...core import settings, sql_achemy_engine

router = APIRouter(
    prefix='/dreams',
    tags=['DREAMS']
)


@router.get(
    '/agyw_prevTableI',
    response_description=settings.AGYWPREVTABI_DESCRIPTION,
    summary=settings.AGYWPREVTABI_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def agyw_prevTableI():
    engine = sql_achemy_engine()
    if engine:
        AGYWPREVTABI_SCHEMA = run_agywprevI(engine)
        json_datim =  dumps(AGYWPREVTABI_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")

@router.get(
    '/agyw_prevTableII',
    response_description=settings.AGYWPREVTABII_DESCRIPTION,
    summary=settings.AGYWPREVTABII_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def agyw_prevTableII():
    engine = sql_achemy_engine()
    if engine:
        AGYWPREVTABII_SCHEMA = run_agywprevII(engine)
        json_datim =  dumps(AGYWPREVTABII_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")

@router.get(
    '/agyw_prevTableIII',
    response_description=settings.AGYWPREVTABIII_DESCRIPTION,
    summary=settings.AGYWPREVTABIII_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def agyw_prevTableIII():
    engine = sql_achemy_engine()
    if engine:
        AGYWPREVTABIII_SCHEMA = run_agywprevIII(engine)
        json_datim =  dumps(AGYWPREVTABIII_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")

@router.get(
    '/agyw_prevTableIV',
    response_description=settings.AGYWPREVTABIV_DESCRIPTION,
    summary=settings.AGYWPREVTABIV_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def agyw_prevTableIV():
    engine = sql_achemy_engine()
    if engine:
        AGYWPREVTABIV_SCHEMA = run_agywprevIV(engine) 
        json_datim =  dumps(AGYWPREVTABIV_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")

@router.get(
    '/vital_info',
    response_description=settings.VITAL_DESCRIPTION,
    summary=settings.VITAL_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def vital_info():
    engine = sql_achemy_engine()
    if engine:
        VITAL_SCHEMA = run_vital_info(engine)  
        json_datim =  dumps(VITAL_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_datim)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


""" @router.get(
    '/district',
    response_description=settings.DISTRICT_DESCRIPTION,
    summary=settings.DISTRICT_SUMMARY,
    status_code=status.HTTP_200_OK
)
async def district():
    json_district = jsonable_encoder(DISTRICT_SCHEMA)
    if json_district:
        return JSONResponse(content=json_district)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong") """
