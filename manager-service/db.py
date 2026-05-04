from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg2
import os


@dataclass(slots=True)
class DatabaseConfig:
    """ host: str = "localhost"
    database: str = "jobsdb"
    user: str = "admin"
    password: str = "admin"
    port: int = 5432 """
    host: str = os.environ["POSTGRES_HOST"]
    database: str = os.environ["POSTGRES_JOBS_DB"]
    user: str = os.environ["POSTGRES_USER"]
    password: str = os.environ["POSTGRES_PASSWORD"]
    port: int = os.environ["POSTGRES_PORT"]


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
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = %s
                    WHERE job_id = %s
                    RETURNING job_id, status, created_at;
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
