from typing import List
from fastapi import APIRouter, Depends, HTTPException
from fastapi.params import Body
from ..db.data import retrieveDataByFormId, update_data,retrieveDataByFormId_not_sent,retrieveDataByFormId_not_sent_regardless_data_out
from ..db.form import createForm, retrieveForm,update_form,deleteForm, createForms, retrieveForms, get_form_by_id, updateForms
from ..db.question import get_questions_by_form_id
from ..models.form import Form, UpdateFormModel
from ..dependencies import get_current_user_from_token
from ..core.config import settings
from ..utils.data import dataInToDataOut
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
    return JSONResponse(content=str(result.inserted_id), status_code=201)

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
    results = await retrieveForms()
    results = [ Form.parse_obj(result) for result in results]
    return results

@router.get("/tsform/")
async def transform_data_in_to_data_out(form_name:str,form_type:str):
    try:
        result = await retrieveForm(form_name,form_type)
        questions = await get_questions_by_form_id(result["_id"])
        if result is not None:
            __res = await retrieveDataByFormId_not_sent_regardless_data_out(result["_id"])
            if __res is not None:
                for  __res_item in __res:
                    __data_out = dataInToDataOut(__res_item["data_in"],result["format_in"],result["format_out"],questions)
                    __update_data = await update_data(__res_item["_id"],{"data_out":__data_out})
                __res_new = await retrieveDataByFormId_not_sent_regardless_data_out(result["_id"], limit=10)

                return JSONResponse(content=__res_new)
            raise HTTPException(status_code=404, detail="Form data not found")
        raise HTTPException(status_code=404, detail="Form not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)+type(e).__name__)