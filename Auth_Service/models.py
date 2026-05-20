from database import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP, text

class Users(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index = True)
    username = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False, index=True)
    role = Column(String, default="USER", index = True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=text('now()')) 