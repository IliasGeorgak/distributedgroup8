from fastapi import FastAPI, HTTPException, UploadFile, File, Form, BackgroundTasks
from pathlib import Path
import os, shutil, tempfile
from manager import ManagerService
import db

app = FastAPI()

DEFAULT_BUCKET = os.getenv("MANAGER_DEFAULT_BUCKET", "mapreduce")
SUPPORTED_INPUT_SUFFIXES = {
    suffix.strip().lower()
    for suffix in os.getenv("MANAGER_SUPPORTED_INPUT_SUFFIXES", ".txt,.jsonl,.json").split(",")
    if suffix.strip()
}
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
    operation: str = Form("word_count"),
    input_format: str = Form("auto"),
    partition_function: str = Form("sha256"),
    bucket_name: str = Form(DEFAULT_BUCKET),
):
    if split_count <= 0:
        raise HTTPException(status_code=400, detail="split_count must be >= 1")

    if r_partitions <= 0:
        raise HTTPException(status_code=400, detail="r_partitions must be >= 1")

    suffix = Path(input_file.filename or "input.txt").suffix or ".txt"
    if suffix.lower() not in SUPPORTED_INPUT_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_INPUT_SUFFIXES))
        raise HTTPException(status_code=400, detail=f"Supported input files: {supported}")

    manager = ManagerService()

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
            operation=operation,
            input_format=input_format,
            partition_function=partition_function,
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit job: {exc}",
        ) from exc

    finally:
        input_path.unlink(missing_ok=True)
