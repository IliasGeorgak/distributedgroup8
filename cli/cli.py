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


def jobs_list() -> None:
    token = load_token()
    if not token:
        print("You are not logged in. Run: python cli.py auth login --username ... --password ...")
        return

    response = requests.get(
        f"{UI_SERVICE_URL}/jobs",
        headers={"Authorization": f"Bearer {token}"},
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

    jobs_parser = subparsers.add_parser("jobs")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command")
    jobs_subparsers.add_parser("list")

    args = parser.parse_args()

    if args.command == "auth" and args.auth_command == "login":
        login(args.username, args.password)
    elif args.command == "auth" and args.auth_command == "logout":
        logout()
    elif args.command == "jobs" and args.jobs_command == "list":
        jobs_list()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
