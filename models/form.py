from typing import List,Optional
from pydantic import BaseModel, Field, EmailStr

from models.question import Question

class Form(BaseModel):
    name :str = Field(..., title="name of the form")
    type: str = Field(..., title="type of the form")
    url_out :str = Field(..., title="url_out: Url to forward the form")
    questions_url_out: str = Field(..., title="questions_url_out: Url to forward the questions")
    format_out :Optional[dict] = Field(None ,title="The format of the output data to url_out")
    format_in :Optional[dict] = Field(None, title="The format of the input data to the form")
    unique_fields :List[str] = Field(..., title="unique_fields: The unique fields of the form")