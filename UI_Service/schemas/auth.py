from pydantic import BaseModel

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str

class UserCreateRequest(BaseModel):
    username: str
    password: str
    email: str
    role: str
    
class UserCreateRequest2(BaseModel):
    username: str
    password: str
    email: str
