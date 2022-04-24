import os
from fastapi import (
    FastAPI,
    Body,
    HTTPException,
    status,
    Depends
)
from fastapi.responses import JSONResponse, FileResponse
import pandas as pd
import json
from typing import Optional
from .models.user import User, RegisterUser, RegisterAdmin
from .models.token import Token, TokenData
import pymongo as pm
from .db import db
from .utils.data import dataInToDataOut
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from .core.config import settings
from .core.hashing import Hasher
from .routers.form_data import router as data_router
from .routers.questions import router as questions_router
from .routers.form import router as form_router
from .routers.login import router as login_router
from .routers.register import router as register_router
from .routers.admin import router as admin_router
from .dependencies import oauth2_scheme, get_current_user_from_token, authenticate_user,create_access_token
from mangum import Mangum

app = FastAPI(title=settings.PROJECT_TITLE, description=settings.PORJECT_DESCRIPTION, version=settings.PROJECT_VERSION, docs_url=settings.DOCS_URL)

app.include_router(data_router)
app.include_router(questions_router)
app.include_router(form_router)
app.include_router(login_router)
app.include_router(register_router)
app.include_router(admin_router)


_forms = db["forms"]


# @app.get("/", tags=['Home'])
# async def root():
#     return {"message": "Please write /docs into the browser path to enter to openapi"}


handler = Mangum(app=app)












