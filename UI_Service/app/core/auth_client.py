from fastapi import Header, HTTPException, Depends, status
from dotenv import load_dotenv
import requests, os

load_dotenv()

host = os.environ["AUTH_HOST"]
port = os.environ["AUTH_PORT"]

AUTH_SERVICE_VALIDATE_URL = f"http://{host}:{port}/validate"

def get_current_user(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = authorization.split(" ", 1)[1]

    try:
        response = requests.get(
            f"{AUTH_SERVICE_VALIDATE_URL}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5
        )
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="Authentication service unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid token")

    data = response.json()

    if not data.get("valid"):
        raise HTTPException(status_code=401, detail="Invalid token")

    return data["user"]

def get_current_admin(current_user=Depends(get_current_user)):
    if current_user.get("role") != "ADMIN":
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return current_user
