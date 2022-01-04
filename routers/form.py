from typing import List
from fastapi import APIRouter, Depends, HTTPException
from db.form import createForm, retrieveForm,updateForm,deleteForm, createForms, retrieveForms
from models.form import Form
from dependencies import get_current_user_from_token
from core.config import settings
from utils.data import dataInToDataOut
from fastapi.responses import JSONResponse

router = APIRouter(
    prefix="/form",
    tags=["Form"],
    dependencies=[Depends(get_current_user_from_token)]
)



@router.post("/")
async def create_form(form_data: Form):
    result = await createForm(form_data)
    return JSONResponse(content=result.dict())

@router.put("/{id}")
async def update_form(form_data: Form):
    result = await updateForm(form_data)
    return JSONResponse(content=result.dict())

@router.post("s/")
async def create_forms(forms: List[Form]):
    result = await createForms(forms)
    return result

@router.get("s/", response_model=List[Form],response_description=settings.FORMS_DESCRIPTION, status_code=201, summary=settings.FORMS_SUMMARY, tags=['Form'])
async def retrieve_forms():
    result = await retrieveForms()
    return result

@router.put("/tsform", tags=['Form'])
async def transform_data_in_to_data_out(name:str,type:str):
    result = await retrieveForm(name,type)
    result_1 = dataInToDataOut(result.get("data_in"),result.get('format_in'),result.get("format_out"))
    return JSONResponse(content=result_1)
