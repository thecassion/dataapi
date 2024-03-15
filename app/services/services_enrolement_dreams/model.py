from pydantic import BaseModel
from typing import List, Union


class ValueScreenedEnrolled(BaseModel):
    fiscal_year: str
    screened: int
    enrolled: int
    percentage_agyw_enrolled: Union[float, int]
    percentage_agyw_enrolled_str: str


class ScreenedEnrolled(BaseModel):
    title: str
    description: str
    values: List[ValueScreenedEnrolled]


class ValueEnrolledInactif(BaseModel):
    fiscal_year: str
    inactif: int
    enrolled: int
    percentage_agyw_inactif: Union[float, int]
    percentage_agyw_inactif_tr: str


class EnrolledInactif(BaseModel):
    title: str
    description: str
    values: List[ValueEnrolledInactif]


class ValuePerTrimester(BaseModel):
    semester_fiscal_year: str
    data: int
    notation: str


class ServedPerTrimester(BaseModel):
    title: str
    description: str
    values: List[ValuePerTrimester]
