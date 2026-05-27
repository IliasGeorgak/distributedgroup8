from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg2
import os


@dataclass(slots=True)
class DatabaseConfig:
    host: str = os.environ["POSTGRES_HOST"]
    database: str = os.environ["POSTGRES_JOBS_DB"]
    user: str = os.environ["POSTGRES_USER"]
    password: str = os.environ["POSTGRES_PASSWORD"]
    port: int = int(os.environ["POSTGRES_PORT"])


class Database:
    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self.config = config or DatabaseConfig()

    def get_connection(self):
        return psycopg2.connect(
            host=self.config.host,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            port=self.config.port,
        )

    def test_connection(self) -> tuple[int]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
        if result is None:
            raise RuntimeError("Database connection test returned no result")
        return result

    def init_schema(self) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id SERIAL PRIMARY KEY,
                        status TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS input_bucket TEXT;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS input_object TEXT;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS split_count INTEGER;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS r_partitions INTEGER;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS case_sensitive BOOLEAN DEFAULT FALSE;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS operation TEXT DEFAULT 'word_count';")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS input_format TEXT DEFAULT 'auto';")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS partition_function TEXT DEFAULT 'sha256';")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP;")
                cursor.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;")

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tasks (
                        task_id SERIAL PRIMARY KEY,
                        job_id INTEGER,
                        task_type TEXT DEFAULT 'map',
                        status TEXT,
                        worker_id TEXT,
                        retry_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                cursor.execute(
                    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_type TEXT DEFAULT 'map';"
                )
                cursor.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS worker_id TEXT;")
                cursor.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS external_task_id TEXT;")
                cursor.execute("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;")
                cursor.execute(
                    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"
                )
                cursor.execute(
                    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"
                )
            conn.commit()

    def create_job(self, status: str = "pending") -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO jobs (status) VALUES (%s) RETURNING job_id;",
                    (status,),
                )
                row = cursor.fetchone()
            conn.commit()
        if row is None:
            raise RuntimeError("Failed to create job")
        return int(row[0])

    def create_tasks(
        self,
        job_id: int,
        num_tasks: int,
        status: str = "pending",
        task_type: str = "map",
    ) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for _ in range(num_tasks):
                    cursor.execute(
                        "INSERT INTO tasks (job_id, task_type, status) VALUES (%s, %s, %s);",
                        (job_id, task_type, status),
                    )
            conn.commit()

    def create_task_records(
        self,
        job_id: int,
        tasks: list[dict[str, Any]],
        status: str = "pending",
        task_type: str | None = None,
    ) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                if task_type is not None:
                    cursor.execute(
                        "DELETE FROM tasks WHERE job_id = %s AND task_type = %s;",
                        (job_id, task_type),
                    )
                for task in tasks:
                    cursor.execute(
                        """
                        INSERT INTO tasks (
                            job_id,
                            task_type,
                            status,
                            external_task_id
                        )
                        VALUES (%s, %s, %s, %s);
                        """,
                        (
                            job_id,
                            task_type or str(task["task_type"]),
                            status,
                            str(task["task_id"]),
                        ),
                    )
            conn.commit()

    def update_task_status(self, task_id: int, status: str) -> dict[str, Any]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE tasks
                    SET status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE task_id = %s
                    RETURNING task_id, job_id, task_type, status, worker_id, retry_count;
                    """,
                    (status, task_id),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            raise ValueError(f"Unknown task_id '{task_id}'")
        return self._task_row_to_dict(row)

    def mark_task_running_by_external_id(
        self,
        external_task_id: str,
        worker_id: str | None,
    ) -> dict[str, Any] | None:
        return self._update_task_by_external_id(
            external_task_id=external_task_id,
            status="running",
            worker_id=worker_id,
        )

    def mark_task_completed_by_external_id(
        self,
        external_task_id: str,
        worker_id: str | None,
    ) -> dict[str, Any] | None:
        return self._update_task_by_external_id(
            external_task_id=external_task_id,
            status="completed",
            worker_id=worker_id,
        )

    def record_task_failure_by_external_id(
        self,
        external_task_id: str,
        worker_id: str | None,
        max_attempts: int,
    ) -> dict[str, Any] | None:
        failed_status = "failed"
        retry_status = "pending"
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE tasks
                    SET retry_count = COALESCE(retry_count, 0) + 1,
                        status = CASE
                            WHEN COALESCE(retry_count, 0) + 1 >= %s THEN %s
                            ELSE %s
                        END,
                        worker_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE external_task_id = %s
                      AND COALESCE(status, '') <> 'completed'
                    RETURNING task_id, job_id, task_type, status, worker_id, retry_count;
                    """,
                    (
                        max_attempts,
                        failed_status,
                        retry_status,
                        worker_id,
                        external_task_id,
                    ),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            return None
        return self._task_row_to_dict(row)

    def _update_task_by_external_id(
        self,
        external_task_id: str,
        status: str,
        worker_id: str | None,
    ) -> dict[str, Any] | None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE tasks
                    SET status = %s,
                        worker_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE external_task_id = %s
                    RETURNING task_id, job_id, task_type, status, worker_id, retry_count;
                    """,
                    (status, worker_id, external_task_id),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            return None
        return self._task_row_to_dict(row)

    def get_pending_tasks(self, task_type: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT task_id, job_id, task_type, status, worker_id, retry_count
            FROM tasks
            WHERE status = %s
        """
        params: list[Any] = ["pending"]

        if task_type is not None:
            query += " AND task_type = %s"
            params.append(task_type)

        query += " ORDER BY created_at, task_id;"

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    query,
                    tuple(params),
                )
                rows = cursor.fetchall()

        return [self._task_row_to_dict(row) for row in rows]

    def mark_task_failed(self, task_id: int, worker_id: str | None = None) -> dict[str, Any]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE tasks
                    SET status = %s,
                        worker_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE task_id = %s
                      AND COALESCE(status, '') <> %s
                    RETURNING task_id, job_id, task_type, status, worker_id, retry_count;
                    """,
                    ("failed", worker_id, task_id, "completed"),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            raise ValueError(f"Task '{task_id}' is already completed or does not exist")
        return self._task_row_to_dict(row)

    def update_job_status(self, job_id: int, status: str) -> dict[str, Any]:
        started_at_sql = (
            ", started_at = COALESCE(started_at, CURRENT_TIMESTAMP)"
            if status in {"running", "running_map", "running_reduce"}
            else ""
        )
        completed_at_sql = (
            ", completed_at = CURRENT_TIMESTAMP"
            if status in {"completed", "failed"}
            else ""
        )

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        updated_at = CURRENT_TIMESTAMP
                        {started_at_sql}
                        {completed_at_sql}
                    WHERE job_id = %s
                    RETURNING job_id, status, created_at, started_at, completed_at,
                              EXTRACT(EPOCH FROM (
                                  COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at
                              ));
                    """,
                    (status, job_id),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            raise ValueError(f"Unknown job_id '{job_id}'")
        return {
            "job_id": row[0],
            "status": row[1],
            "created_at": row[2],
            "started_at": row[3],
            "completed_at": row[4],
            "duration_seconds": float(row[5]) if row[5] is not None else None,
        }

    def recover_stale_running_jobs(self, stale_after_seconds: int) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'submitted',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status IN ('running', 'running_map', 'map_completed', 'running_reduce')
                      AND updated_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second');
                    """,
                    (stale_after_seconds,),
                )
                recovered_count = cursor.rowcount
            conn.commit()
        return int(recovered_count)

    def recover_retryable_failed_jobs(
        self,
        max_attempts: int,
        retry_delay_seconds: int,
    ) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'submitted',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status = 'failed'
                      AND COALESCE(retry_count, 0) < %s
                      AND updated_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second');
                    """,
                    (max_attempts, retry_delay_seconds),
                )
                recovered_count = cursor.rowcount
            conn.commit()
        return int(recovered_count)

    def record_job_failure(self, job_id: int, max_attempts: int) -> dict[str, Any]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET retry_count = COALESCE(retry_count, 0) + 1,
                        status = CASE
                            WHEN COALESCE(retry_count, 0) + 1 < %s
                            THEN 'submitted'
                            ELSE 'failed'
                        END,
                        completed_at = CASE
                            WHEN COALESCE(retry_count, 0) + 1 < %s
                            THEN completed_at
                            ELSE CURRENT_TIMESTAMP
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = %s
                    RETURNING job_id, status, created_at, started_at, completed_at,
                              EXTRACT(EPOCH FROM (
                                  COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at
                              ));
                    """,
                    (max_attempts, max_attempts, job_id),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            raise ValueError(f"Unknown job_id '{job_id}'")
        return {
            "job_id": row[0],
            "status": row[1],
            "created_at": row[2],
            "started_at": row[3],
            "completed_at": row[4],
            "duration_seconds": float(row[5]) if row[5] is not None else None,
        }

    @staticmethod
    def _task_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
        return {
            "task_id": row[0],
            "job_id": row[1],
            "task_type": row[2],
            "status": row[3],
            "worker_id": row[4],
            "retry_count": row[5],
        }
    def get_job_status(self, job_id: int) -> dict:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT job_id, status, created_at, started_at, completed_at,
                           EXTRACT(EPOCH FROM (
                               COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at
                           ))
                    FROM jobs
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
                row = cursor.fetchone()

            if row is None:
                raise ValueError(f"Unknown job_id '{job_id}")
            
            return {
                "job_id": row[0],
                "status": row[1],
                "created_at": row[2],
                "started_at": row[3],
                "completed_at": row[4],
                "duration_seconds": float(row[5]) if row[5] is not None else None,
            }
        
    def get_job_tasks_status(self, job_id: int) -> dict:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 
                    j.job_id,
                    j.status as job_status,
                    j.created_at,
                    t.task_type,
                    t.status AS task_status,
                    COUNT(t.task_id) AS task_count
                    FROM jobs j
                    LEFT JOIN tasks t ON j.job_id = t.job_id
                    WHERE j.job_id = %s
                    GROUP BY
                        j.job_id,
                        j.status,
                        j.created_at,
                        t.task_type,
                        t.status;
                    """,
                    (job_id,),

                )
                rows = cursor.fetchall()
        
        if not rows:
            raise ValueError(f"Job {job_id} not found!")

        first = rows[0]

        return {
            "job_id": first[0],
            "status": first[1],
            "created_at": first[2],
            "tasks": [
                {
                    "task_type": row[3],
                    "status": row[4],
                    "count": row[5]
                }
                for row in rows
                if row[3] is not None            
            ],
        }
    ###############
    def create_submitted_job(
        self,
        input_bucket: str,
        input_object: str,
        split_count: int,
        r_partitions: int,
        case_sensitive: bool,
        operation: str = "word_count",
        input_format: str = "auto",
        partition_function: str = "sha256",
    ) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO jobs (
                        status,
                        input_bucket,
                        input_object,
                        split_count,
                        r_partitions,
                        case_sensitive,
                        operation,
                        input_format,
                        partition_function
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING job_id;
                    """,
                    (
                        "submitted",
                        input_bucket,
                        input_object,
                        split_count,
                        r_partitions,
                        case_sensitive,
                        operation,
                        input_format,
                        partition_function,
                    ),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            raise RuntimeError("Failed to create submitted job")

        return int(row[0])

    def claim_next_submitted_job(self) -> dict[str, Any] | None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'running',
                        started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = (
                        SELECT job_id
                        FROM jobs
                        WHERE status = 'submitted'
                        ORDER BY created_at
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING job_id, status, input_bucket, input_object,
                            split_count, r_partitions, case_sensitive,
                            operation, input_format, partition_function;
                    """
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            return None

        return {
            "job_id": row[0],
            "status": row[1],
            "input_bucket": row[2],
            "input_object": row[3],
            "split_count": row[4],
            "r_partitions": row[5],
            "case_sensitive": row[6],
            "operation": row[7],
            "input_format": row[8],
            "partition_function": row[9],
        }
    
    def update_job_submission_metadata(
        self,
        job_id: int,
        status: str,
        input_bucket: str,
        input_object: str,
        split_count: int,
        r_partitions: int,
        case_sensitive: bool,
        operation: str,
        input_format: str,
        partition_function: str,
    ) -> dict[str, Any]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        input_bucket = %s,
                        input_object = %s,
                        split_count = %s,
                        r_partitions = %s,
                        case_sensitive = %s,
                        operation = %s,
                        input_format = %s,
                        partition_function = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = %s
                    RETURNING job_id, status, input_bucket, input_object,
                            split_count, r_partitions, case_sensitive,
                            operation, input_format, partition_function;
                    """,
                    (
                        status,
                        input_bucket,
                        input_object,
                        split_count,
                        r_partitions,
                        case_sensitive,
                        operation,
                        input_format,
                        partition_function,
                        job_id,
                    ),
                )
                row = cursor.fetchone()
            conn.commit()

        if row is None:
            raise ValueError(f"Unknown job_id '{job_id}'")

        return {
            "job_id": row[0],
            "status": row[1],
            "input_bucket": row[2],
            "input_object": row[3],
            "split_count": row[4],
            "r_partitions": row[5],
            "case_sensitive": row[6],
            "operation": row[7],
            "input_format": row[8],
            "partition_function": row[9],
        }
