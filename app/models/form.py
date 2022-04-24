from typing import List,Optional
from pydantic import BaseModel, Field, EmailStr
from ..utils.typing import PyObjectId
from bson import ObjectId

from ..models.question import Question

class Form(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, title="The unique identifier of the form", description="The unique identifier of the form", alias="_id")
    name :str = Field(..., title="name of the form")
    version :Optional[float] = Field(None , title="version of the form")
    type: str = Field(..., title="type of the form")
    url_out :str = Field(..., title="url_out: Url to forward the form")
    questions_url_out: str = Field(..., title="questions_url_out: Url to forward the questions")
    questions_url_in: str = Field(..., title="questions_url_in: Url to get from api the questions")
    format_out :Optional[dict] = Field(None ,title="The format of the output data to url_out")
    format_in :Optional[dict] = Field(None, title="The format of the input data to the form")
    unique_fields :Optional[List[str]] = Field(None, title="unique_fields: The unique fields of the form")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "name": "form_name",
                "type": "form_type",
                "version": 1.0,
                "url_out": "http://url_out",
                "questions_url_out": "http://questions_url_out",
                "format_out": {
                    "codeEcole": "site_identifier_12",
                    "directeurDtos": [
                        {
                            "directeurDto": {
                                "nomComplet": "string",
                                "nif": "string",
                                "nin": "string",
                                "tel": "telephone_directeur",
                                "code": "string"
                            },
                            "questionReponse": [
                                {"repeat":"questions"},
                                {
                                    "uuidQuestion": "code",
                                    "reponse": "form.data_in[code]"
                                }
                            ]
                        }
                    ]
                },
                "format_in": {}
            }
        }

class UpdateFormModel(BaseModel):
    name :Optional[str]
    type: Optional[str]
    version :Optional[float] = Field(None , title="version of the form")
    url_out :Optional[str]
    questions_url_out: Optional[str]
    questions_url_in: Optional[str]
    format_out :Optional[dict]
    format_in :Optional[dict]
    unique_fields :Optional[List[str]]

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "name": "form_name",
                "type": "form_type",
                "version": 1.0,
                "url_out": "http://url_out",
                "questions_url_out": "http://questions_url_out",
                "format_out": {
                    "codeEcole": "site_identifier_12",
                    "directeurDtos": [
                        {
                            "directeurDto": {
                                "nomComplet": "string",
                                "nif": "string",
                                "nin": "string",
                                "tel": "telephone_directeur",
                                "code": "string"
                            },
                            "questionReponse": [
                                {"repeat":"questions"},
                                {
                                    "uuidQuestion": "code",
                                    "reponse": "form[code]"
                                }
                            ]
                        }
                    ]
                },
                "format_in": {}
            }
        }
