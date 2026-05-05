from pydantic import BaseModel

class UserCreateRequest(BaseModel):
    username: str
    password: str
    email: str
    role: str

class UserCreateRequest2(BaseModel):
    username: str
    password: str
    email: str

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: str

    class Config:
        from_attributes = True
    