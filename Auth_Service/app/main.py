from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from app.database.database import Base, engine, get_db
from sqlalchemy.orm import Session
from app.models import Users
from app.schemas.user import UserResponse, UserCreateRequest, UserCreateRequest2
from app.schemas.token import Token
from typing import List
from app.core.oauth2 import get_pwd_hash, verify_pwd, create_access_token, get_current_user, get_current_admin
from datetime import timedelta, timezone, datetime
from app.config import settings 



app = FastAPI()

Base.metadata.create_all(bind=engine)


@app.get("/")
def home():
    return {"message" : "Hello World"}

@app.get("/profile", response_model=UserResponse)
def get_profile(current_user:Users = Depends(get_current_user)):
    return current_user

@app.get("/validate")
def validate_token(current_user:Users = Depends(get_current_user)):
    return {
        "valid": True,
        "user": {
            "id": current_user.id,
            "username": current_user.username,
            "email": current_user.email,
            "role": current_user.role
        }
    }


@app.get("/users/{user_id}", response_model=UserResponse)
def get_user(user_id:int, current_user:Users = Depends(get_current_admin), db:Session = Depends(get_db)):
    user = db.query(Users).filter(Users.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found!")
    return user

@app.post("/users", response_model=UserResponse)
def create_user(user:UserCreateRequest, current_user:Users = Depends(get_current_admin), db:Session = Depends(get_db)):
    if db.query(Users).filter(Users.email == user.email).first():
        raise HTTPException(status_code=404,detail="User already exists!")

    hashed_password = get_pwd_hash(user.password)
    db_user = Users(
        username = user.username,
        hashed_password = hashed_password,
        email = user.email,
        role = user.role
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

@app.delete("/users/{user_id}")
def delete_user(user_id:int, current_user:Users = Depends(get_current_admin), db:Session = Depends(get_db)):
    db_user = db.query(Users).filter(Users.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found!")
    if current_user.id == user_id:
        raise HTTPException(status_code=404,detail="Cannot delete current user!")
    
    db.delete(db_user)
    db.commit()
    return {"message": "User deleted successfully!"}

@app.get("/users", response_model=List[UserResponse])
def get_all_users(current_user:Users = Depends(get_current_admin), db:Session = Depends(get_db)):
    return db.query(Users).all()

@app.post("/register", response_model=UserResponse)
def register_user(user: UserCreateRequest2, db: Session = Depends(get_db)):
    if db.query(Users).filter(Users.email == user.email).first():
        raise HTTPException(
            status_code=404,
            detail="User already created!"
        )
    hashed_password = get_pwd_hash(user.password)
    db_user = Users(
        username = user.username,
        hashed_password = hashed_password,
        email = user.email,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

@app.post("/token", response_model=Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(Users).filter(Users.username == form_data.username).first()

    if not user or not verify_pwd(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=404,
            detail="Invalid credentials!"
        )
    
    access_token_expires = timedelta(minutes= settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data = {"sub":user.email, "role":user.role}, expires_delta=access_token_expires
    )
    return {"access_token":access_token,"token_type":"bearer"}
