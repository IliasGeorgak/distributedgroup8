from minio import Minio 

client = Minio(
    "localhost:9000",  # use port-forward
    access_key="admin",
    secret_key="password123",
    secure=False
)


def test_connection():
    buckets = client.list_buckets()
    print("MinIO connection works")
    for bucket in buckets:
        print("Bucket:", bucket.name)


def ensure_bucket(bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket already exists: {bucket_name}")


def upload_file(bucket_name: str, object_name: str, file_path: str):
    client.fput_object(bucket_name, object_name, file_path)
    print(f"Uploaded {file_path} as {object_name} to bucket {bucket_name}")