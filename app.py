import os
from fastapi import FastAPI, Body, HTTPException, status,File, UploadFile, Form
from fastapi.responses import JSONResponse, FileResponse
import pandas as pd
import json

from starlette.responses import StreamingResponse
from models.question import UpdateQuestions, Questions
from models.form import Form
from models.user import ShowUser, UserCreate
from typing import  List
import pymongo as pm
from db import db
from db.form import createForms, retrieveForm, updateForm,createForm, retrieveForms
from db.question import create_question, get_questions_by_form
from db.user import createUser, retrieveUser, retrieveUsers
from utils.data import dataInToDataOut
import io
from datetime import datetime
from core.config import settings

app = FastAPI(title=settings.PROJECT_TITLE, description=settings.PORJECT_DESCRIPTION, version=settings.PROJECT_VERSION)


_forms = db["forms"]
_forms.create_index([("type",pm.ASCENDING),("name",pm.ASCENDING)], unique=True,name="form_index")
_form_data_in = db["form_data_in"]
_form_data_out = db["form_data_out"]

@app.get("/", tags=['Home'])
async def root():
    return {"message": "Please write /docs into the browser path to enter to openapi"}

@app.post("/bulksuploadformdata", response_model=List[dict],response_description=settings.BULKUPLOADFORMDATA_DESCRIPTION, status_code=201, summary=settings.BULKUPLOADFORMDATA_SUMMARY,tags=['data processing'])
async def form(file: UploadFile = File(...)):
    df = pd.read_excel(file.file.read(), header=1)
    df["_id"] = df.site_identifier_11
    df["_id_site_identifier_12"]=df.site_identifier_12
    df.site_identifier_12 = df.site_identifier_12.astype(str)
    _json = json.loads(df.to_json(orient='records'))
    await _forms.insert_many(_json)
    return JSONResponse(content=_json)

@app.post("/questions",summary=settings.QUESTIONS_SUMMARY,response_description=settings.QUESTIONS_DESCRIPTION, status_code=201, tags=['Question'])
async def questions(questions: Questions):
    result = await create_question(questions)
    return result

@app.put("/questions",summary=settings.QUESTION_UPDATE_SUMMARY, response_description=settings.QUESTION_UPDATE_DESCRIPTION, tags=['Question'])
async def update_questions(questions: UpdateQuestions):
    return {"questions": "questions"}

@app.post("/form")
async def create_form(form_data: Form):
    result = await createForm(form_data)
    return JSONResponse(content=result.dict())

@app.put("/form")
async def update_form(form_data: Form):
    result = await updateForm(form_data)
    return JSONResponse(content=result.dict())

@app.post("/forms")
async def create_forms(forms: List[Form]):
    result = await createForms(forms)
    return result

@app.get("/forms", response_model=List[Form],response_description="Create a list of forms on our server and the output server", status_code=201, summary="Create a list of forms on our server and the output server")
async def retrieve_forms():
    result = await retrieveForms()
    return result

@app.put("/tsform")
async def transform_data_in_to_data_out(name:str,type:str):
    result = await retrieveForm(name,type)
    result_1 = dataInToDataOut(result.get("data_in"),result.get('format_in'),result.get("format_out"))
    return JSONResponse(content=result_1)

@app.post("/checkform/xlsx")
async def check_form(name:str,type:str, file : UploadFile = File(...)):
    df = pd.read_excel(file.file.read())
    df = df[:1]
    __df_col = df.transpose()
    __df_col["description"] = __df_col.index
    __df_col.columns = ["code","description"]
    old_questions = await get_questions_by_form(name,type)
    if isinstance(old_questions,dict):
        if old_questions["message"] == "Form not found":
            return {"message":"Form not found"}
    if len(old_questions) > 0:
        __df_old_questions = pd.DataFrame(old_questions)
        __old_join_new_by_description = pd.merge(__df_col,__df_old_questions,on="description",how="left")
        __new_join_old_by_description = pd.merge(__df_old_questions,__df_col,on="description",how="left")
        __new_join_old_by_code = pd.merge(__df_col,__df_old_questions,on="code",how="left")
        __old_join_new_by_code = pd.merge(__df_old_questions,__df_col,on="code",how="left")
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        __df_col.to_excel(writer, sheet_name="new", index=False)
        if len(old_questions) > 0:
            __df_old_questions.to_excel(writer, sheet_name="old", index=False)
            __old_join_new_by_description.to_excel(writer, sheet_name="old_join_new_by_description", index=False)
            __new_join_old_by_description.to_excel(writer, sheet_name="new_join_old_by_description", index=False)
            __new_join_old_by_code.to_excel(writer, sheet_name="new_join_old_by_code", index=False)
            __old_join_new_by_code.to_excel(writer, sheet_name="old_join_new_by_code", index=False)
    buffer.seek(0)
    # Download the file
    headers = {"Content-Disposition": "attachment; filename="+type+"_"+name+"_"+str(datetime.now())+".xlsx"}
    return StreamingResponse(buffer,headers=headers)

@app.post("/form/questions/xlsx", response_description="Create a list of questions for a specific form using an excel ", status_code=201, summary="Create a list of questions on our server using an excel")
async def column_data_from_xlsx(name:str,type:str, file : UploadFile = File(...)):
    result = await retrieveForm(name,type)
    df = pd.read_excel(file.file.read())
    df = df[:1]
    __df_col = df.transpose()
    __df_col["description"] = __df_col.index
    __df_col.columns = ["code","description"]
    old_questions = await get_questions_by_form(name,type)
    __df_old_questions = pd.DataFrame(old_questions)
    __df_col.to_excel(type+"_"+name+".xlsx")
    __df_old_questions.to_excel(type+"_"+name+".xlsx")

    __my_json = json.loads(__df_col.to_json(orient='records'))
    return __my_json

