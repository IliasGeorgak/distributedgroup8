from pydantic import BaseModel
from typing import Optional

class UserLoginRequest(BaseModel):
    username: str
    password: str

