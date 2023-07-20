from pydantic import BaseModel
from typing import List, Dict, Any


class EidStatusModel(BaseModel):
    titles: Dict[str, List[Any]]
    pcr_status: List[Dict[str, List[Any]]]
    positivity_status: List[Dict[str, Any]]
    liaison_mere_status: List[Dict[str, int]]
