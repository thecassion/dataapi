from fastapi import (
    APIRouter,
    status,
    HTTPException
)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

""" from ...services.dreams_services import (
    datim_function,
    #district_function,
    #DISTRICT_SCHEMA,
    datim_object
) """

from ...services.services_dreams import run_datim

from ...core import settings, sql_achemy_engine

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
    engine = sql_achemy_engine()
    if engine:
        DATIM_SCHEMA = run_datim(engine)
        json_datim =  jsonable_encoder(DATIM_SCHEMA)
        return JSONResponse(content=json_datim)  
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
