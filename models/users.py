from pydantic import BaseModel
from typing import List, Optional

class Company(BaseModel):
    ticker: str
    cik: str
    title: str

class User(BaseModel):
    id: str
    name: str
    watchlist: Optional[List[Company]]
    webhook: str