from db import  get_connection
from db import init_db;
from db import create_job
from db import create_tasks
from storage import test_connection , ensure_bucket

conn =  get_connection()
print("connected to database")

cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())

init_db()
print("Database initialized")


job_id = create_job()
print("Created job:", job_id)

job_id = create_job()
create_tasks(job_id, 3)
print("Created job with tasks")

test_connection()

ensure_bucket("betet")