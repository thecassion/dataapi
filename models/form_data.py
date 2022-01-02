from typing import List, Optional
from pydantic import BaseModel, Field, EmailStr



class FormData(BaseModel):
    data_in: dict = Field(..., title="data_in: The data to be inserted in the form")
    data_out: str = Field(..., title="form_name: The name of the form")
