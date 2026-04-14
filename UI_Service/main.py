from fastapi import FastAPI, Depends, HTTPException
from schemas.auth import LoginRequest
from dependencies.auth_client import get_current_user, get_current_admin
import requests

AUTH_SERVICE_URL = "http://localhost:8080"
AUTH_SERVICE_LOGIN_URL = "http://localhost:8080/token"

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