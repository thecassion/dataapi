from fastapi import (
    APIRouter, 
    Depends,
    status,
    HTTPException   
)
from datetime import timedelta
from fastapi.security import( 
    OAuth2PasswordBearer, 
    OAuth2PasswordRequestForm
)
from ..core.config import settings
from ..dependencies import (
    authenticate_user,
    create_access_token,
)
from ..models.token import Token



router = APIRouter(
    prefix="/token",
    tags=['LOGIN']   
)


@router.post("/" , response_description=settings.LOGIN_DESCRIPTION, summary=settings.LOGIN_SUMMARY, response_model=Token, status_code=status.HTTP_201_CREATED)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    username = form_data.username
    password = form_data.password
    _user = await authenticate_user(username, password)
    if not _user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": username}, expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token, "token_type": "bearer"}



