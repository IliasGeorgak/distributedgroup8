# cli.py
import argparse
import json
from pathlib import Path

import requests

UI_SERVICE_URL = "http://localhost:8081"
TOKEN_FILE = Path(".mapreduce_token.json")


def save_token(token_data: dict) -> None:
    TOKEN_FILE.write_text(json.dumps(token_data, indent=2), encoding="utf-8")


def load_token() -> str | None:
    if not TOKEN_FILE.exists():
        return None

    data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    return data.get("access_token")

def get_auth_headers() -> dict[str, str] | None:
    token = load_token()
    if not token:
        print("You are not logged in. Run: python cli.py auth login --username ... --password ...")
        return None

    return {"Authorization": f"Bearer {token}"}

def login(username: str, password: str) -> None:
    response = requests.post(
        f"{UI_SERVICE_URL}/auth/login",
        json={
            "username": username,
            "password": password,
        },
        timeout=5,
    )

    if response.status_code != 200:
        print("Login failed:", response.text)
        return

    token_data = response.json()
    save_token(token_data)
    print("Login successful.")
    print("Token stored locally.")

def register(username: str, password: str, email: str) -> None:
    payload = {
        "username": username,
        "password": password,
        "email": email,
    }

    response = requests.post(
        f"{UI_SERVICE_URL}/auth/register",
        json=payload,
        timeout=5,
    )

    print("Status:", response.status_code)
    print(response.text)

def jobs_list() -> None:
    headers = get_auth_headers()
    if headers is None:
        return

    response = requests.get(
        f"{UI_SERVICE_URL}/jobs",
        headers=headers,
        timeout=5,
    )

    print("Status:", response.status_code)
    print(response.text)

def admin_create_user(username: str, password: str, email: str, role: str) -> None:
    headers = get_auth_headers()
    if headers is None:
        return

    payload = {
        "username": username,
        "password": password,
        "email": email,
        "role": role,
    }

    response = requests.post(
        f"{UI_SERVICE_URL}/admin/users",
        json=payload,
        headers=headers,
        timeout=5,
    )

    print("Status:", response.status_code)
    print(response.text)

def admin_delete_user(user_id: int) -> None:
    headers = get_auth_headers()
    if headers is None:
        return
    response = requests.delete(
        f"{UI_SERVICE_URL}/users/{user_id}",
        headers=headers,
        timeout=5
    )
    
    print("Status:", response.status_code)
    print(response.text)
    
def admin_view_users() -> None:
    headers = get_auth_headers()
    if headers is None:
        return

    response = requests.get(
        f"{UI_SERVICE_URL}/admin/users",
        headers=headers,
        timeout=5,
    )

    print("Status:", response.status_code)
    print(response.text)

def logout() -> None:
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
    print("Logged out.")

def main() -> None:
    parser = argparse.ArgumentParser(description="MapReduce CLI")
    subparsers = parser.add_subparsers(dest="command")

    auth_parser = subparsers.add_parser("auth")
    auth_subparsers = auth_parser.add_subparsers(dest="auth_command")

    login_parser = auth_subparsers.add_parser("login")
    login_parser.add_argument("--username", required=True)
    login_parser.add_argument("--password", required=True)

    auth_subparsers.add_parser("logout")

    register_parser = auth_subparsers.add_parser("register")
    register_parser.add_argument("--username", required=True)
    register_parser.add_argument("--password", required=True)
    register_parser.add_argument("--email", required=True)

    jobs_parser = subparsers.add_parser("jobs")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command")
    jobs_subparsers.add_parser("list")

    admin_parser = subparsers.add_parser("admin")
    admin_subparsers = admin_parser.add_subparsers(dest="admin_command")

    admin_create_user_parser = admin_subparsers.add_parser("create_user")
    admin_create_user_parser.add_argument("--username", required=True)
    admin_create_user_parser.add_argument("--password", required=True)
    admin_create_user_parser.add_argument("--email", required=True)
    admin_create_user_parser.add_argument("--role", required=True)

    admin_delete_parser = admin_subparsers.add_parser("delete_user")
    admin_delete_parser.add_argument("--user_id", required=True, type=int)
    
    admin_subparsers.add_parser("view_users")

    args = parser.parse_args()

    if args.command == "auth" and args.auth_command == "login":
        login(args.username, args.password)
    elif args.command == "auth" and args.auth_command == "logout":
        logout()
    elif args.command == 'auth' and args.auth_command == "register":
        register(args.username, args.password, args.email)
    elif args.command == "jobs" and args.jobs_command == "list":
        jobs_list()
    elif args.command == "admin" and args.admin_command == "create_user":
        admin_create_user(args.username, args.password, args.email, args.role)
    elif args.command == "admin" and args.admin_command == "delete_user":
        admin_delete_user(args.user_id)   
    elif args.command == "admin" and args.admin_command == "view_users":
        admin_view_users()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
