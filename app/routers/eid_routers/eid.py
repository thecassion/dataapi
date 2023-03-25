from fastapi import (
    APIRouter,
    status,
    HTTPException
)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from json import dumps
from numpyencoder import NumpyEncoder
from fastapi_filter import FilterDepends

from ...services.services_eid import summary_eid_total, Eid_filter

from ...core import settings, sql_achemy_engine

router = APIRouter(
    prefix='/eid',
    tags=['EID']
)

@router.get(
    '/summary',
    #response_description=settings.DATIM_DESCRIPTION,
    response_description="information globale pour le testing eid",
    #summary=settings.DATIM_SUMMARY,
    summary="information globale pour le testing eid",
    status_code=status.HTTP_200_OK
)
async def eid():
    engine = sql_achemy_engine()
    if engine:
        eid_SCHEMA = summary_eid_total(engine)
        json_eid =  dumps(eid_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_eid)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@router.get(
    '/summary/filter',
    #response_description=settings.DATIM_DESCRIPTION,
    response_description="filtrage des informations importantes pour eid",
    #summary=settings.DATIM_SUMMARY,
    summary="filtrage des informations importantes pour eid",
    status_code=status.HTTP_200_OK
)
async def eid_filter(eid_filter:Eid_filter = FilterDepends(Eid_filter)):
    engine = sql_achemy_engine()
    if engine:
        eid_SCHEMA = summary_eid_total(engine,year=eid_filter.year, quarter=eid_filter.quarter,network=eid_filter.network)
        json_eid =  dumps(eid_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_eid)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


""" @router.get(
    '/year/{year}',
    #response_description=settings.DATIM_DESCRIPTION,
    response_description="information par annee  pour le testing eid",
    #summary=settings.DATIM_SUMMARY,
    summary="information par annee  pour le testing eid",
    status_code=status.HTTP_200_OK
)
async def eid_by_year(year:int):
    engine = sql_achemy_engine()
    if engine:
        eid_SCHEMA = summary_eid_total(engine,year=year)
        json_eid =  dumps(eid_SCHEMA, cls=NumpyEncoder).encode('utf-8')
        return Response(media_type="application/json", content=json_eid)
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong") """
