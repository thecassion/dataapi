from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List
from itertools import groupby
from operator import itemgetter


class SchoolingPositif(BaseModel):
    departement: str
    commune: str
    total: int
    male: int
    female: int
    unknown_gender: int
    f_under_1: int
    f_1_4: int
    f_5_9: int
    f_10_14: int
    f_15_17: int
    f_18_20: int
    f_over_20: int
    m_under_1: int
    m_1_4: int
    m_5_9: int
    m_10_14: int
    m_15_17: int
    m_18_20: int
    m_over_20: int

    # Method calculate the quarter based on dat_peyman_fet
    @property
    def calculate_quarter(dat_peyman_fet):
        date_peye = datetime.strptime(dat_peyman_fet, "%Y-%m-%d")
        quarter = (date_peye.month - 1) // 3 + 1
        year = date_peye.year
        return f"Q{quarter}{year}"

    # Function calculate the the category of age
    @property
    def calculate_age_category(age: int) -> str:
        if age < 1:
            return "<1"
        elif 1 <= age < 5:
            return "1-4"
        elif 5 <= age < 10:
            return "5-9"
        elif 10 <= age < 15:
            return "10-14"
        elif 15 <= age < 18:
            return "15-17"
        elif 18 <= age < 20:
            return "18-20"
        else:
            return '20+'
