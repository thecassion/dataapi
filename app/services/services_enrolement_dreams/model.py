from pydantic import BaseModel
from typing import List, Union


class ValueScreenedEligible(BaseModel):
    fiscal_year: str
    screened: int
    eligible: int
    percentage_agyw_enrolled: Union[float, int]


class ScreenedVsEligible(BaseModel):
    title: str
    values: List[ValueScreenedEligible]


class ValueEligibleToBeServed(BaseModel):
    fiscal_year: str
    to_be_served: int
    eligible: int
    percentage_agyw_to_be_served: Union[float, int]


class EligibleVsToBeServed(BaseModel):
    title: str
    values: List[ValueEligibleToBeServed]


class ValuePerTrimester(BaseModel):
    semester_fiscal_year: str
    data: int
    notation: str


class ServedPerTrimester(BaseModel):
    title: str
    values: List[ValuePerTrimester]
