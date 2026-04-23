import time
import numpy
import psycopg2
import os

dbname = os.environ["POSTGRES_DB"]
user = os.environ["POSTGRES_USER"]
password = os.environ["postgres-password"]
host = os.environ["POSTGRES_HOST"]
port = os.environ["POSTGRES_PORT"]

conn = psycopg2.connect(
    dbname,
    user,
    password,
    host,
    port
)


# Create a cursor
cur = conn.cursor()
with open("schema.sql", "r", encoding="utf-8") as f:
    sql = f.read()
# Create table
cur.execute(sql)
f.close()
with open("test_data.sql", "r", encoding="utf-8") as f:
    sql = f.read()
# Create table
cur.execute(sql)
f.close()
with open("verify.sql", "r", encoding="utf-8") as f:
    sql = f.read()
# Create table
cur.execute(sql)
rows = cur.fetchall()
f.close()


for row in rows:
    print(row)

# Commit changes
conn.commit()

# Close connections
cur.close()
conn.close()