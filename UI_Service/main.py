from fastapi import FastAPI, Depends, HTTPException, Header
from schemas.auth import LoginRequest, UserCreateRequest
from dependencies.auth_client import get_current_user, get_current_admin
import requests
import os

host = os.environ["AUTH_HOST"]
port = os.environ["AUTH_PORT"]
AUTH_SERVICE_URL = f"http://{host}:{port}"
AUTH_SERVICE_LOGIN_URL = f"http://{host}:{port}/token"


app = FastAPI()

@app.get("/")
def home():
    return {"Hello":"World"}

@app.post("/auth/login")
def login(data: LoginRequest):
    try:
        response = requests.post(
            f"{AUTH_SERVICE_LOGIN_URL}",
            data={
                "username": data.username,
                "password": data.password
            },
            timeout=5
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Authentication service unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return response.json()

@app.get("/admin/users")
def list_users(current_user=Depends(get_current_admin), authorization: str | None = Header(default=None)):
    try:
        response = requests.get(
            f"{AUTH_SERVICE_URL}/users",
            headers={"Authorization": authorization},
            timeout=5
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Authentication service unavailiable!")
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()

@app.post("/admin/users")
def create_user(user:UserCreateRequest,
                current_user=Depends(get_current_admin),
                authorization: str | None = Header(default=None)
):
    try:
        response = requests.post(
            f"{AUTH_SERVICE_URL}/users",
            json=user.model_dump(),
            headers={"Authorization": authorization},
            timeout=5
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Authentication service unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=401, detail=response.text)

    return response.json()

    

@app.delete("admin/users/{user_id}")
def delete_user(
        user_id:int,
        current_user=Depends(get_current_admin),
        authorization: str | None = Header(default=None),
):
    try:
        response = requests.delete(
            f"{AUTH_SERVICE_URL}/users/{user_id}",
            headers={"Authorization": authorization},
            timeout=5
        )

    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Authorization service unavailiable!")
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    return response.json()


@app.get("/profile")
def profile(current_user=Depends(get_current_user)):
    return {
        "message": "Authenticated user",
        "user": current_user
    }

@app.get("/jobs")
def list_jobs(current_user=Depends(get_current_user)):
    return {"message": f"Hello {current_user['username']}"}

@app.get("/admin/test")
def admin_test(current_user=Depends(get_current_admin)):
    return {"message": "Admin access granted"}
