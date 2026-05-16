from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pathlib import Path
import os, shutil, tempfile
from manager import ManagerService
import db

app = FastAPI()

DEFAULT_BUCKET = os.getenv("MANAGER_DEFAULT_BUCKET", "mapreduce")
database =db.Database()

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

    try:
        manager.database.init_schema()
        manager.storage.ensure_bucket(bucket_name)
        job_id = manager.database.create_job(status="submitted")

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Manager dependencies unavailable: {exc}",
        ) from exc

    suffix = Path(input_file.filename or "input.txt").suffix or ".txt"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        shutil.copyfileobj(input_file.file, temp_file)
        input_path = Path(temp_file.name)

    try:
        original_object = f"inputs/job-{job_id}/input{suffix}"
        manager.upload_input_file(
            bucket_name=bucket_name,
            object_name=original_object,
            file_path=input_path,
        )

        split_dir = Path(tempfile.mkdtemp(prefix=f"job-{job_id}-splits-"))

        split_paths, split_objects, map_tasks = manager.prepare_map_tasks_from_input_file(
            job_id=job_id,
            input_file=input_path,
            input_bucket=bucket_name,
            output_bucket=bucket_name,
            split_count=split_count,
            split_dir=split_dir,
            split_object_prefix=f"inputs/job-{job_id}/splits",
            map_parameters={
                "case_sensitive": case_sensitive,
                "r_partitions": r_partitions,
                "partition_function": "md5",
            },
        )

        manager.database.create_tasks(
            job_id=job_id,
            num_tasks=len(map_tasks),
            status="pending",
            task_type="map",
        )

    except Exception as exc:
        manager.database.update_job_status(job_id, "failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit job: {exc}",
        ) from exc

    finally:
        input_path.unlink(missing_ok=True)

    return {
        "job_id": job_id,
        "status": "submitted",
        "input": {
            "bucket": bucket_name,
            "object": original_object,
        },
        "splits": [
            {
                "object": object_name,
            }
            for object_name in split_objects
        ],
        "map_task_count": len(map_tasks),
        "split_count": split_count,
        "r_partitions": r_partitions,
        "case_sensitive": case_sensitive,
        "message": "Job submitted. Kubernetes worker job creation is handled separately.",
    }
