from fastapi import FastAPI, Depends, HTTPException, Header, UploadFile, Form, File
from app.schemas.auth import LoginRequest, UserCreateRequest, UserCreateRequest2
from app.core.auth_client import get_current_user, get_current_admin
from dotenv import load_dotenv
import requests
import os

load_dotenv()

host = os.environ["AUTH_HOST"] 
port = os.environ["AUTH_PORT"] #8080
AUTH_SERVICE_URL = f"http://{host}:{port}"
AUTH_SERVICE_LOGIN_URL = f"http://{host}:{port}/token"
AUTH_SERVICE_REGISTER_URL = f"http://{host}:{port}/register"
MANAGER_SERVICE_URL = os.getenv("MANAGER_SERVICE_URL", "http://manager-service:8000")

app = FastAPI()

@app.get("/")
def home():
    return {"Hello":"World"}

@app.post("/auth/register")
def register(data: UserCreateRequest2):
    try:
        response = requests.post(
            f"{AUTH_SERVICE_REGISTER_URL}",
            json=data.model_dump(),   
            timeout=5,
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Authentication service unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()

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

    
@app.delete("/admin/users/{user_id}")
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

@app.post("/jobs/submit_job")
def submit_job(
    input_file: UploadFile = File(...),
    split_count: int = Form(4),
    r_partitions: int = Form(3),
    case_sensitive: bool = Form(False),
    current_user=Depends(get_current_user),
):
    try:
        response = requests.post(
            f"{MANAGER_SERVICE_URL}/jobs/submit_job",
            files={
                "input_file": (
                    input_file.filename,
                    input_file.file,
                    input_file.content_type or "text/plain",
                )
            },
            data={
                "split_count": str(split_count),
                "r_partitions": str(r_partitions),
                "case_sensitive": str(case_sensitive).lower(),
            },
            timeout=30,
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Manager service unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()

@app.get("/jobs/{job_id}")
def get_job_status(
    job_id: int,
    current_user=Depends(get_current_user),
):
    try:
        response = requests.get(
            f"{MANAGER_SERVICE_URL}/jobs/{job_id}",
            timeout=5,
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Manager service unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()

@app.get("/jobs/{job_id}/results")
def get_job_results(
    job_id: int,
    current_user=Depends(get_current_user),
):
    try:
        response = requests.get(
            f"{MANAGER_SERVICE_URL}/jobs/{job_id}/results",
            timeout=10,
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Manager service unavailable")

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
