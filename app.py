import os
from fastapi import FastAPI, Body, HTTPException, status,File, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from bson import ObjectId
import pandas as pd
import io
import json
import motor.motor_asyncio
from models.question import UpadateQuestion, Question
from models.form import Form
from typing import  List

# import pymongo as pm

app = FastAPI(title="UNOPS DATA INTEGRATION", description="A data integration system that helps UNOPS send their data to USI system", version="0.1")



client = motor.motor_asyncio.AsyncIOMotorClient(os.environ.get('MONGODB_URI'))
db = client["unops"]
_forms = db["forms"]


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/bulksuploadformdata", response_model=List[dict],response_description="Create form data_in in the server using an xlsx, json , csv or xml format", status_code=201, summary="Create form data_in in the server using an xlsx, json , csv or xml format")
async def form(file: UploadFile = File(...)):
    df = pd.read_excel(file.file.read(), header=1)
    df["_id"] = df.site_identifier_11
    df["_id_site_identifier_12"]=df.site_identifier_12
    df.site_identifier_12 = df.site_identifier_12.astype(str)
    _json = json.loads(df.to_json(orient='records'))
    await _forms.insert_many(_json)
    return JSONResponse(content=_json)

@app.post("/questions",summary="create a list of questions on our server and the output server", response_model=List[dict],response_description="Create a list of questions on our server and the output server", status_code=201)
async def questions(questions: List[Question]):

    return {"questions": "questions"}

@app.put("/questions")
async def update_questions(questions: List[UpadateQuestion]):

    return {"questions": "questions"}

@app.post("/url_out")
async def url_out(url: str):
    return {"url": url}

@app.post("/form")
async def create_form(form_data: Form):
    await _forms.insert_one(form_data.dict())
    return JSONResponse(content=form_data.dict(), status_code=201)

@app.post("/forms")
async def create_forms(forms: List[Form]):
    _forms.insert_many(forms)
    return {"forms": forms}
