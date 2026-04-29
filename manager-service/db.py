from __future__ import annotations

from dataclasses import dataclass

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
                        status TEXT
                    );
                    """
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

    def create_tasks(self, job_id: int, num_tasks: int, status: str = "pending") -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for _ in range(num_tasks):
                    cursor.execute(
                        "INSERT INTO tasks (job_id, status) VALUES (%s, %s);",
                        (job_id, status),
                    )
            conn.commit()
















