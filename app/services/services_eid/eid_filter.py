import fastapi_filter.contrib.sqlalchemy  as filter

from typing import Optional

class Eid_filter(filter.Filter):
    year: Optional[int]
    quarter: Optional[str]
    network: Optional[str]


class EidStatus_filter(filter.Filter):
    year: Optional[int]
    office: Optional[str]
    hospital: Optional[str]


