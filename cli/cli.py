# cli.py
import argparse, json, base64, time, requests
from pathlib import Path

UI_SERVICE_URL = "http://localhost:8081"
TOKEN_FILE = Path(".mapreduce_token.json")

def decode_jwt_payload(token: str) -> dict:
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def token_expires_soon(token: str, threshold_seconds: int = 60) -> bool:
    try:
        payload = decode_jwt_payload(token)
        exp = int(payload["exp"])
        return exp - time.time() < threshold_seconds
    except Exception:
        return True


def refresh_token() -> bool:
    token = load_token()
    if not token:
        return False

    response = requests.post(
        f"{UI_SERVICE_URL}/auth/refresh",
        headers={"Authorization": f"Bearer {token}"},
        timeout=5,
    )

    if response.status_code != 200:
        return False

    save_token(response.json())
    return True

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
        print("You are not logged in. Run: python cli.py auth login -u ... -p ...")
        return None

    if token_expires_soon(token):
        if not refresh_token():
            print("Session expired. Run: python cli.py auth login -u ... -p ...")
            return None
        token = load_token()

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

def jobs_submit(input_file: str, split_count: int, r_partitions: int, case_sensitive: bool) -> None:
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
                "input_file": (input_path.name, file, "text/plain"),
            },
            data={
                "split_count": str(split_count),
                "r_partitions": str(r_partitions),
                "case_sensitive": str(case_sensitive).lower(),
            },
            timeout=30,
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
        timeout=5,
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
        timeout=10,
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
    login_parser.add_argument("-u", "--username", required=True)
    login_parser.add_argument("-p", "--password", required=True)

    auth_subparsers.add_parser("logout")

    register_parser = auth_subparsers.add_parser("register")
    register_parser.add_argument("-u", "--username", required=True)
    register_parser.add_argument("-p", "--password", required=True)
    register_parser.add_argument("-e", "--email", required=True)

    jobs_parser = subparsers.add_parser("jobs")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command")
    jobs_subparsers.add_parser("list")

    jobs_submit_parser = jobs_subparsers.add_parser("submit")
    jobs_submit_parser.add_argument("-i", "--input_file", required=True)
    jobs_submit_parser.add_argument("-m", "--split_count", type=int, default=4)
    jobs_submit_parser.add_argument("-r", "--r_partitions", type=int, default=3)
    jobs_submit_parser.add_argument("-c", "--case_sensitive", action="store_true")

    jobs_status_parser = jobs_subparsers.add_parser("status")
    jobs_status_parser.add_argument("-id", "--job_id", required=True, type=int)

    jobs_results_parser = jobs_subparsers.add_parser("results")
    jobs_results_parser.add_argument("-id", "--job_id", required=True, type=int)

    admin_parser = subparsers.add_parser("admin")
    admin_subparsers = admin_parser.add_subparsers(dest="admin_command")

    admin_create_user_parser = admin_subparsers.add_parser("create_user")
    admin_create_user_parser.add_argument("-u", "--username", required=True)
    admin_create_user_parser.add_argument("-p", "--password", required=True)
    admin_create_user_parser.add_argument("-e", "--email", required=True)
    admin_create_user_parser.add_argument("-r", "--role", required=True)

    admin_delete_parser = admin_subparsers.add_parser("delete_user")
    admin_delete_parser.add_argument("-id", "--user_id", required=True, type=int)
    
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
        )
    elif args.command == "jobs" and args.jobs_command == "status":
        jobs_status(args.job_id)
    elif args.command == "jobs" and args.jobs_command == "results":
        jobs_results(args.job_id)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
