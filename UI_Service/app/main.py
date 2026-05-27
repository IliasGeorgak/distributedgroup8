from fastapi import FastAPI, Depends, HTTPException, Header, UploadFile, Form, File
from app.schemas.auth import LoginRequest, UserCreateRequest, UserCreateRequest2
from app.core.auth_client import get_current_user, get_current_admin
from dotenv import load_dotenv
import logging
import requests
import os
<<<<<<< HEAD
from requests_toolbelt import MultipartEncoder
=======
from urllib.parse import urlparse
from threading import Lock
>>>>>>> load_balancing

load_dotenv()

host = os.environ["AUTH_HOST"] 
port = os.environ["AUTH_PORT"] #8080
AUTH_SERVICE_URL = f"http://{host}:{port}"
AUTH_SERVICE_LOGIN_URL = f"http://{host}:{port}/token"
AUTH_SERVICE_REGISTER_URL = f"http://{host}:{port}/register"
MANAGER_SERVICE_URL = os.getenv("MANAGER_SERVICE_URL", "http://manager-service:8000")
<<<<<<< HEAD
MANAGER_REQUEST_TIMEOUT_SECONDS = float(os.getenv("UI_MANAGER_REQUEST_TIMEOUT_SECONDS", "900"))
=======
MANAGER_REPLICAS = [
    url.strip().rstrip("/")
    for url in os.getenv("MANAGER_REPLICAS", "").split(",")
    if url.strip()
]
MANAGER_REQUEST_TIMEOUT_SECONDS = float(os.getenv("MANAGER_REQUEST_TIMEOUT_SECONDS", "30"))

logger = logging.getLogger("ui.manager_lb")
logger.setLevel(logging.INFO)
_manager_replica_lock = Lock()
_manager_replica_index = 0
>>>>>>> load_balancing

app = FastAPI()


def _manager_submit_candidates() -> list[str]:
    global _manager_replica_index

    if not MANAGER_REPLICAS:
        return [MANAGER_SERVICE_URL.rstrip("/")]

    with _manager_replica_lock:
        selected_index = _manager_replica_index
        _manager_replica_index = (_manager_replica_index + 1) % len(MANAGER_REPLICAS)

    ordered_replicas = (
        MANAGER_REPLICAS[selected_index:]
        + MANAGER_REPLICAS[:selected_index]
    )
    fallback_url = MANAGER_SERVICE_URL.rstrip("/")
    if fallback_url not in ordered_replicas:
        ordered_replicas.append(fallback_url)

    selected_replica_name = urlparse(ordered_replicas[0]).hostname or ordered_replicas[0]
    logger.info("Selected manager replica: %s", selected_replica_name)
    print(f"Selected manager replica: {selected_replica_name}", flush=True)
    return ordered_replicas


def _rewind_uploaded_files(uploaded_files: list[UploadFile]) -> None:
    for uploaded_file in uploaded_files:
        uploaded_file.file.seek(0)


def _manager_files_payload(uploaded_files: list[UploadFile]):
    return [
        (
            "input_files" if len(uploaded_files) > 1 else "input_file",
            (
                uploaded_file.filename,
                uploaded_file.file,
                uploaded_file.content_type or "text/plain",
            ),
        )
        for uploaded_file in uploaded_files
    ]

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
    input_file: UploadFile | None = File(None),
    input_files: list[UploadFile] | None = File(None),
    split_count: int = Form(4),
    r_partitions: int = Form(3),
    case_sensitive: bool = Form(False),
    operation: str = Form("word_count"),
    input_format: str = Form("auto"),
    partition_function: str = Form("sha256"),
    current_user=Depends(get_current_user),
):
    uploaded_files = input_files or ([input_file] if input_file is not None else [])
    if not uploaded_files:
        raise HTTPException(status_code=400, detail="At least one input file is required")

<<<<<<< HEAD
    fields = [
        (
            "input_files" if len(uploaded_files) > 1 else "input_file",
            (
                uploaded_file.filename,
                uploaded_file.file,
                uploaded_file.content_type or "text/plain",
            ),
        )
        for uploaded_file in uploaded_files
    ]

    try:
        multipart = MultipartEncoder(
            fields=[
                *fields,
                ("split_count", str(split_count)),
                ("r_partitions", str(r_partitions)),
                ("case_sensitive", str(case_sensitive).lower()),
                ("operation", operation),
                ("input_format", input_format),
                ("partition_function", partition_function),
            ]
        )
        response = requests.post(
            f"{MANAGER_SERVICE_URL}/jobs/submit_job",
            data=multipart,
            headers={"Content-Type": multipart.content_type},
            timeout=MANAGER_REQUEST_TIMEOUT_SECONDS,
=======
    data = {
        "split_count": str(split_count),
        "r_partitions": str(r_partitions),
        "case_sensitive": str(case_sensitive).lower(),
        "operation": operation,
        "input_format": input_format,
        "partition_function": partition_function,
    }

    response = None
    manager_errors: list[str] = []
    for manager_url in _manager_submit_candidates():
        logger.info("Forwarding job submission to manager: %s", manager_url)
        try:
            _rewind_uploaded_files(uploaded_files)
            response = requests.post(
                f"{manager_url}/jobs/submit_job",
                files=_manager_files_payload(uploaded_files),
                data=data,
                timeout=MANAGER_REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code in {502, 503, 504}:
                manager_errors.append(f"{manager_url}: HTTP {response.status_code}")
                logger.warning(
                    "Manager replica returned unavailable status for job submission: %s (%s)",
                    manager_url,
                    response.status_code,
                )
                response = None
                continue
            break
        except requests.RequestException as exc:
            manager_errors.append(f"{manager_url}: {exc}")
            logger.warning(
                "Manager replica unavailable for job submission: %s (%s)",
                manager_url,
                exc,
            )

    if response is None:
        raise HTTPException(
            status_code=503,
            detail="Manager service unavailable; tried: " + "; ".join(manager_errors),
>>>>>>> load_balancing
        )

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
