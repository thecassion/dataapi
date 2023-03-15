from enum import Enum
from datetime import datetime


class Set_date(Enum):
    master_start = "2017-10-01"
    master_end = datetime.today().strftime('%Y-%m-%d')
    period_start = "2021-10-01"
    period_end = datetime.today().strftime('%Y-%m-%d')
