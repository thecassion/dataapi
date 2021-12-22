from pydantic import BaseModel, Field, EmailStr
from typing import Collection, Optional, List
from bson.objectid import ObjectId as BsonObjectId


class PydanticObjectId(BsonObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, BsonObjectId):
            raise TypeError('ObjectId required')
        return str(v)


class UpadateQuestion(BaseModel):
    code: Optional[str] = Field(..., title="name of the question")
    uid: str=Field(..., title="guuid of the question")
    description: Optional[str]=Field(None, title="label of the question")
    path: Optional[str]=Field(None, title="path of the question")

class Question(BaseModel):
    code: str=Field(..., title="name of the question")
    description: str=Field(None, title="label of the question")
    path: Optional[str]=Field(None, title="path of the question")
    uid: Optional[str]=Field(None, title="guuid of the question")
    form: Optional[PydanticObjectId]=Field(None, title="form of the question")

class UpdateQuestions(BaseModel):
    questions: List[UpadateQuestion] = Field(..., title="list of questions to be updated")
    form_name: str = Field(..., title="name of the form")
    form_type: str = Field(..., title="type of the form")
class Questions(BaseModel):
    questions: List[Question] = Field(..., title="list of questions")
    form_name: str = Field(..., title="name of the form")
    form_type: str = Field(..., title="type of the form")