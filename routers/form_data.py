from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from db.data import createData
from db.form import retrieveForm
from models.form_data import FormData
import pandas as pd
import json
router = APIRouter(
    prefix="/form/data",
    tags=["Data Processing"]
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
                    __dict = df.to_dict(orient='records')
                    result = await createData(__dict)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result