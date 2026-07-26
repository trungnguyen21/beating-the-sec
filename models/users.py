from pydantic import BaseModel, field_validator
from typing import List, Optional

class Company(BaseModel):
    ticker: str
    cik: str
    title: str

    @field_validator("cik", mode="before")
    @classmethod
    def pad_cik(cls, v) -> str:
        return str(int(v)).zfill(10)

class User(BaseModel):
    id: str
    name: str
    watchlist: Optional[List[Company]]
    webhook: str