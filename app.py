import os
from fastapi import FastAPI, Body, HTTPException, status,File, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, EmailStr
# from bson import ObjectId
from typing import Optional, List
import tempfile
import pandas as pd
import io
import json

app = FastAPI()
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}

@app.post("/form/")
async def form(file: UploadFile = File(...)):
    df = pd.read_excel(file.file.read(), header=1)
    df.to_excel("tmp.xlsx")
    _json = json.loads(df.to_json(orient='records'))
    return JSONResponse(content=_json)