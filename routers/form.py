from typing import List
from fastapi import APIRouter, Depends, HTTPException
from fastapi.params import Body
from db.form import createForm, retrieveForm,update_form,deleteForm, createForms, retrieveForms, get_form_by_id, updateForms
from models.form import Form, UpdateFormModel
from dependencies import get_current_user_from_token
from core.config import settings
from utils.data import dataInToDataOut
from fastapi.responses import JSONResponse

router = APIRouter(
    prefix="/form",
    tags=["Form"],
    dependencies=[Depends(get_current_user_from_token)]
)


@router.get("/{id}", response_model=Form)
async def form_by_id(id: str):
    try:
        if (form := await get_form_by_id(id)) is not None:
            return form
        else:
            raise HTTPException(status_code=404, detail="Form not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/")
async def create_form(form_data: Form):
    result = await createForm(form_data)
    return JSONResponse(content=result.dict())

@router.put("/{id}", response_description="Update a Form", response_model=Form)
async def update_a_form(id:str,form: UpdateFormModel = Body(...)):
    try:
        result = await update_form(id,form)
        if result is not None:
            return result
        else:
            raise HTTPException(status_code=404, detail="Form {id} not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
