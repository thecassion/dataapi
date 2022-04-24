from fastapi import (
        status,
        Depends,
        HTTPException
)
from .db.user import(
    getUserByUsername,
    getUserByEmail,
    createUser
)
from fastapi.security import (
    OAuth2PasswordBearer,
    OAuth2PasswordRequestForm
)
from jose import jwt, JWTError
from .core.hashing import Hasher
from datetime import timedelta,datetime
from .core.config import settings
from .models.token import TokenData
from .models.user import User



from .db.user import (
    getUserByUsername,
    getUserByEmail,
    createUser
)
async def authenticate_user(username:str,password:str)->User:
    _user =  await getUserByUsername(username)
    print(_user)
    if not _user:
        return False
    if not Hasher.verify_password(password,_user.password):
        return False
    return _user

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


async def get_current_user_from_token(token:str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials")
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = await getUserByUsername(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_admin_from_token(token:str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials")
    adm_credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate admin credentials")
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = await getUserByUsername(username=token_data.username)
    if user is None:
        raise credentials_exception
    if user.is_superUser==False:
        raise adm_credentials_exception
    return user
