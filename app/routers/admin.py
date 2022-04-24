from fastapi import (
    APIRouter, 
    Depends,
    status,
    HTTPException   
)
from pydantic import EmailStr
from typing import  List, Optional

from ..core.config import settings
from ..models.user import User
from ..db.user import (
    createUser,
    getUsers,
    getUserByEmail,
    getUserByUsername,
    updateUser,
    updateUser_byEmail,
    delUserByEmail,
    delUserByUsername
)
from ..dependencies import get_current_admin_from_token

router = APIRouter(
    prefix="/admin",
    tags=['USER'],
    dependencies=[Depends(get_current_admin_from_token)]
)




@router.get("/user/{username}", response_model=User,response_description=settings.GET_USER_DESCRIPTION, summary=settings.GET_USER_SUMMARY, status_code=status.HTTP_201_CREATED)
async def get_userByUsername(username:str)->User:
    _user = await getUserByUsername(username)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong wwith {username}")





@router.post("/createUser", response_model=User ,response_description=settings.CREATE_USER_DESCRIPTION, summary=settings.CREATE_USER_SUMMARY, status_code=status.HTTP_201_CREATED)
async def post_user(user:User)->User:
    _user = user.dict()
    _resUser = await createUser(_user)
    if _resUser:
        return _resUser
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@router.get("/user/{email}/", response_model=User, response_description=settings.GET_USER_DESCRIPTION, summary=settings.GET_USER_SUMMARY,status_code=status.HTTP_201_CREATED)
async def get_userByEmail(email: EmailStr)->User:
    _user = await getUserByEmail(email)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong wwith {email}")



@router.get('/users', response_model=List[User], response_description=settings.GET_USERS_DESCRIPTION, summary=settings.GET_USERS_SUMMARY,status_code=status.HTTP_201_CREATED)
async def get_users()->List[User]:
    _users = await getUsers()
    if _users:
        return _users
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@router.put("/update_user/{username}", response_model=User,response_description=settings.UPDATE_USER_DESCRIPTION, summary=settings.UPDATE_USER_SUMMARY, status_code=status.HTTP_201_CREATED,  tags=['USER'])
async def put_user(username:str,email:EmailStr,password:str, is_active:Optional[bool], is_superUser:Optional[bool])->User:
    _user = await updateUser(username,email,password,is_active,is_superUser)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {username}")


@router.put("/update_user/{email}/", response_model=User,response_description=settings.UPDATE_USER_DESCRIPTION, summary=settings.UPDATE_USER_SUMMARY, status_code=status.HTTP_201_CREATED,  tags=['USER'])
async def update_user(username:str,email:EmailStr,password:str, is_active:Optional[bool], is_superUser:Optional[bool])->User:
    _user = await updateUser_byEmail(username,email,password,is_active,is_superUser)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {email}")



@router.delete("/delete_user/{email}/", response_description=settings.DELETE_USER_DESCRIPTION, summary=settings.DELETE_USER_SUMMARY, status_code=status.HTTP_201_CREATED)
async def delete_userByEmail(email:EmailStr)-> dict:
    _userdeleted = await delUserByEmail(email)
    if _userdeleted:
        return {"Message": f"the user with the {email} has been deleted" }
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {email}")



@router.delete("/delete_user/{username}", response_description=settings.DELETE_USER_DESCRIPTION, summary=settings.DELETE_USER_SUMMARY, status_code=status.HTTP_201_CREATED)
async def delete_userByUsername(username:str)->dict:
    _userdeleted = await delUserByUsername(username)
    if _userdeleted:
        return {"Message": f"the user with the {username} has been deleted" }
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {username}")













