from fastapi import FastAPI, HTTPException, UploadFile, File, Form, BackgroundTasks
from pathlib import Path
import os, shutil, tempfile
from manager import ManagerService
import db

app = FastAPI()

DEFAULT_BUCKET = os.getenv("MANAGER_DEFAULT_BUCKET", "mapreduce")
database =db.Database()

import threading

@app.on_event("startup")
def start_job_monitor():
    manager = ManagerService()
    thread = threading.Thread(
        target=manager.job_monitor_loop,
        daemon=True,
    )
    thread.start()

@app.get("/health")
def status():
    return {"status": "ok"}

@app.get("/jobs/{job_id}")
def job_status(job_id:int):
    try:
        return database.get_job_status(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

@app.get("/jobs/{job_id}/results")
def job_results(job_id: int, bucket_name: str = DEFAULT_BUCKET):
    manager = ManagerService()
    try:
        return manager.get_job_results(job_id=job_id, bucket_name=bucket_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to retrieve job results: {exc}") from exc

@app.post("/jobs/submit_job")
def submit_job(
    input_file: UploadFile = File(...),
    split_count: int = Form(4),
    r_partitions: int = Form(3),
    case_sensitive: bool = Form(False),
    bucket_name: str = Form(DEFAULT_BUCKET),
):
    if split_count <= 0:
        raise HTTPException(status_code=400, detail="split_count must be >= 1")

    if r_partitions <= 0:
        raise HTTPException(status_code=400, detail="r_partitions must be >= 1")

    if input_file.filename and not input_file.filename.endswith(".txt"):
        raise HTTPException(status_code=400, detail="Only .txt input files are supported")

    manager = ManagerService()

    suffix = Path(input_file.filename or "input.txt").suffix or ".txt"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        shutil.copyfileobj(input_file.file, temp_file)
        input_path = Path(temp_file.name)

    try:
        return manager.submit_input_job(
            input_file=input_path,
            original_filename=input_file.filename,
            bucket_name=bucket_name,
            split_count=split_count,
            r_partitions=r_partitions,
            case_sensitive=case_sensitive,
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit job: {exc}",
        ) from exc

    finally:
        input_path.unlink(missing_ok=True)

