import psycopg2


def get_connection():
    conn = psycopg2.connect(
        host = "localhost",
        database = "jobsdb",
        user = "admin",
        password = "admin",
        port = 5432
    )

    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    print(type(cursor))
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_id SERIAL PRIMARY KEY,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        task_id SERIAL PRIMARY KEY,
        job_id INTEGER,
        status TEXT
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()


def create_job():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute( "INSERT INTO jobs (status) VALUES (%s) RETURNING job_id;",
        ("pending",) )
    
    job_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()

    return job_id

def create_tasks(job_id, num_tasks):
    conn = get_connection()
    cursor = conn.cursor()

    for _ in range(num_tasks):
        cursor.execute(
            "INSERT INTO tasks (job_id, status) VALUES (%s, %s);",
            (job_id, "pending")
        )

    conn.commit()
    cursor.close()
    conn.close()

















