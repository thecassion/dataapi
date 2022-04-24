from fastapi import (
    APIRouter, 
    Depends,
    status,
    HTTPException   
)
from ..core.config import settings
from ..models.user import RegisterAdmin, RegisterUser
from ..db.user import registerAdmin, registerUser


router = APIRouter(
    prefix="/register",
    tags=['Register']
)



@router.post('/', response_model=RegisterUser, response_description=settings.REGISTRATION_DESCRIPTION, summary=settings.REGISTRATION_SUMMARY, status_code=status.HTTP_201_CREATED)
async def register(user: RegisterUser)->RegisterUser:
    _user = user.dict()
    _resUser = await registerUser(_user)
    if _resUser:
        return _resUser
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@router.post('/admin', response_model=RegisterAdmin, response_description=settings.REGISTER_ADMIN_DESCRIPTION, summary=settings.REGISTER_ADMIN_SUMMARY, status_code=status.HTTP_201_CREATED)
async def register_admin(admin: RegisterAdmin)->RegisterAdmin:
    _user = admin.dict()
    _resUser = await registerAdmin(_user)
    if _resUser:
        return _resUser
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")

