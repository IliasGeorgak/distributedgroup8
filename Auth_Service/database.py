from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# db_pass = "Aggelos123@"
# db_port = "5432"
SQLALCHEMY_DATABASE_URL = 'postgresql+psycopg2://postgres:Aggelos123%40@localhost:5432/auth_db'

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()