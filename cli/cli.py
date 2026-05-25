# cli.py
import argparse
import json
import os
from pathlib import Path

import requests

UI_SERVICE_URL = os.environ["MAPREDUCE_UI_SERVICE_URL"].rstrip("/")
TOKEN_FILE = Path(os.getenv("MAPREDUCE_TOKEN_FILE", ".mapreduce_token.json"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("MAPREDUCE_REQUEST_TIMEOUT_SECONDS", "30"))


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
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.status_code != 200:
        print("Login failed:", response.text)
        return

    token_data = response.json()
    save_token(token_data)
    print("Login successful.")
    print("Token stored locally.")

def register(username: str, password: str, email: str) -> None:
    
    if load_token():
        print("You are already logged in! Log out to register as new user!")
        return
    
    payload = {
        "username": username,
        "password": password,
        "email": email,
    }

    response = requests.post(
        f"{UI_SERVICE_URL}/auth/register",
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
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
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    print("Status:", response.status_code)
    print(response.text)

def _content_type_for_path(input_path: Path) -> str:
    if input_path.suffix.lower() == ".json":
        return "application/json"
    if input_path.suffix.lower() == ".jsonl":
        return "application/x-ndjson"
    return "text/plain"


def jobs_submit(
    input_file: str,
    split_count: int,
    r_partitions: int,
    case_sensitive: bool,
    operation: str,
    input_format: str,
    partition_function: str,
) -> None:
    headers = get_auth_headers()
    if headers is None:
        return

    input_path = Path(input_file)
    if not input_path.exists():
        print(f"Input file does not exist: {input_path}")
        return

    with input_path.open("rb") as file:
        response = requests.post(
            f"{UI_SERVICE_URL}/jobs/submit_job",
            headers=headers,
            files={
                "input_file": (input_path.name, file, _content_type_for_path(input_path)),
            },
            data={
                "split_count": str(split_count),
                "r_partitions": str(r_partitions),
                "case_sensitive": str(case_sensitive).lower(),
                "operation": operation,
                "input_format": input_format,
                "partition_function": partition_function,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    print("Status:", response.status_code)
    print(response.text)

def jobs_status(job_id: int) -> None:
    headers = get_auth_headers()
    if headers is None:
        return

    response = requests.get(
        f"{UI_SERVICE_URL}/jobs/{job_id}",
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    print("Status:", response.status_code)
    print(response.text)

def jobs_results(job_id: int) -> None:
    headers = get_auth_headers()
    if headers is None:
        return

    response = requests.get(
        f"{UI_SERVICE_URL}/jobs/{job_id}/results",
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
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
        timeout=REQUEST_TIMEOUT_SECONDS,
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
        timeout=REQUEST_TIMEOUT_SECONDS,
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
        timeout=REQUEST_TIMEOUT_SECONDS,
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

    jobs_submit_parser = jobs_subparsers.add_parser("submit")
    jobs_submit_parser.add_argument("--input_file", required=True)
    jobs_submit_parser.add_argument("--split_count", type=int, default=4)
    jobs_submit_parser.add_argument("--r_partitions", type=int, default=3)
    jobs_submit_parser.add_argument("--case_sensitive", action="store_true")
    jobs_submit_parser.add_argument("--operation", default="word_count")
    jobs_submit_parser.add_argument("--input_format", default="auto")
    jobs_submit_parser.add_argument("--partition_function", default="sha256")

    jobs_status_parser = jobs_subparsers.add_parser("status")
    jobs_status_parser.add_argument("--job_id", required=True, type=int)

    jobs_results_parser = jobs_subparsers.add_parser("results")
    jobs_results_parser.add_argument("--job_id", required=True, type=int)

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
    elif args.command == "jobs" and args.jobs_command == "submit":
        jobs_submit(
            args.input_file,
            args.split_count,
            args.r_partitions,
            args.case_sensitive,
            args.operation,
            args.input_format,
            args.partition_function,
        )
    elif args.command == "jobs" and args.jobs_command == "status":
        jobs_status(args.job_id)
    elif args.command == "jobs" and args.jobs_command == "results":
        jobs_results(args.job_id)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
