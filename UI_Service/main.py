from fastapi import FastAPI, Depends, HTTPException


app = FastAPI()

@app.get("/")
def home():
    return {"Hello":"World"}