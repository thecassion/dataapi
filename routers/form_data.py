from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from db.data import createData, retrieveData
from db.form import retrieveForm
from dependencies import get_current_user_from_token
from models.form_data import FormData
import pandas as pd
import numpy as np
import json
router = APIRouter(
    prefix="/form/data",
    tags=["Data Processing"],
    dependencies=[Depends(get_current_user_from_token)]
)

@router.post("/")
async def create_form_data(form_data: FormData, form_name: str, form_type: str):
    """
    Create a new form data.
    """
    try:
        form_data = FormData(form_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return form_data

@router.post("/xlsx")
async def create_form_data_xlsx(form_type: str, form_name: str, file: UploadFile = File(...)):
    """
    Create new form data.
    """
    try:
        result = await retrieveForm(form_name, form_type)
        if result:
            form = result
            df = pd.read_excel(file.file.read(), header=1)
            if 'unique_fields' in form:
                unique_fields = list(form["unique_fields"])
                if len(unique_fields) > 0:
                    df["_id"] = df[unique_fields].apply(lambda x: '_'.join(x.map(str)), axis=1)
                    df["form_id"] = form["_id"]
                    df = df.drop_duplicates(subset=['_id'])
                    df = df.where((pd.notnull(df)) & pd.notna(df), None)
                    df = df.where(pd.notna(df), None)
                    __dict = df.to_dict(orient='records')
                    print("Type of fields:",type(__dict[0]["nbre_gar_3afc"]))
                    result = await createData(__dict)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result

@router.get("/")
async def get_form_data(form_name: str, form_type: str,limit:int=50, skip:int=0):
    """
    Get all data.

    """
    try:
        result = await retrieveForm(form_name, form_type)
        if result:
            form = result
            __list = await retrieveData(form["_id"],limit,skip)
            return __list
        else:
            raise HTTPException(status_code=404,detail="Form not found") # {"message":"Form not found"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))