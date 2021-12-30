from typing import List
from db import db
from core.hashing import Hasher
from models.user import User, UserUpdate
from typing import Optional

user_collection = db.users

async def createUser(user: dict)-> User:
    _user = dict(user)
    _hashed_password = Hasher.get_password_hash(_user.get('password'))
    _user['password'] = _hashed_password
    _usercreate = await user_collection.insert_one(_user)
    _new_user = await user_collection.find_one({"_id": _usercreate.inserted_id})
    if _new_user:
        return User(**_new_user)
    

async def getUsers()-> List[User]:
    _users = user_collection.find({}).to_list(100000)
    if _users:
        _users = [User(**_user) for _user in await _users]       
        return _users

    
async def getUserByUsername(username: str)->User:
    _user = await user_collection.find_one({"username": username})
    if _user:
        return User(**_user)
    
    

    
async def getUserByEmail(email: str)->User:
    _user = await user_collection.find_one({"email": email})
    if _user:
        return User(**_user)

    


async def updateUser(username:str,email:str,password:str,is_active:Optional[bool],is_superUser:Optional[bool])->User:
    await user_collection.update_one({"username": username}, {
        "$set":{
            "email": email,
            "password": Hasher.get_password_hash(password),
            "is_active": is_active,
            "is_superUser": is_superUser
            }
    })
    _updated_user = await user_collection.find_one({"username": username})
    return User(**_updated_user)


async def updateUser_byEmail(username:str,email:str,password:str,is_active:Optional[bool],is_superUser:Optional[bool])->User:
    await user_collection.update_one({"email": email}, {
        "$set":{
            "username": username,
            "password": Hasher.get_password_hash(password),
            "is_active": is_active,
            "is_superUser": is_superUser
            }
    })
    _updated_user = await user_collection.find_one({"email": email})
    return User(**_updated_user)




async def delUserByEmail(email:str)->bool:
    _user = await user_collection.find_one({"email": email})
    if _user:
        await user_collection.delete_one({"email": email})
        return True




async def delUserByUsername(username:str)->bool:
    _user = await user_collection.find_one({"username": username})
    if _user:
        await user_collection.delete_one({"username": username})
        return True



