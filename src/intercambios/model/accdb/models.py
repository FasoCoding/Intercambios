from pydantic import BaseModel
from datetime import datetime

class Costo_Marginal(BaseModel):
    node: str
    datetime: datetime
    cmg: float