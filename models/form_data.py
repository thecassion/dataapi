from typing import List, Optional
from pydantic import BaseModel, Field, EmailStr



class FormData(BaseModel):
    unique_fields: List[str] = Field(..., title="unique_fields: The unique fields of the form")
    data_in: dict = Field(..., title="data_in: The data to be inserted in the form")
    form_name: str = Field(..., title="form_name: The name of the form")
    form_type: str = Field(..., title="form_type: The type of the form")

