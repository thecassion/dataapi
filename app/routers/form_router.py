from fastapi import (
    APIRouter
)

from ..dataanalysis import (
    FormToDB
)

from pydantic import BaseModel

class Form(BaseModel):
    form_xmlns: str
    table_name: str




router = APIRouter(
    prefix="/form",
    tags=["Form"]
)

@router.post("/savetodb")
def save_form_to_db(form: Form):
    try:
        form_to_db = FormToDB(form.form_xmlns, form.table_name)
        form_to_db.save()
        return {"message":"Form saved to db"}
    except Exception as e:
        print(e)
        return {"message":str(e)}