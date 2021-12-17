
from db import db
from models.form import Form

form_collection = db.forms
async def retrieveForm(name:str,type:str)->Form:
    __form = form_collection.find_one({"name":name,"type":type})
    if __form:
        return Form(__form)