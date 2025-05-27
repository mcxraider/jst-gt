# services/storage.py
import pandas as pd
import os
from pathlib import Path
import io

from config import S3_BUCKET_NAME, AWS_REGION, USE_S3
import boto3

from dotenv import load_dotenv
load_dotenv()  
aws_access_key = os.environ["AWS_ACCESS_KEY"]
aws_secret_key = os.environ["AWS_SECRET_KEY"]


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=AWS_REGION,
    )

def _parse_s3_path(s3_path):
    """ Given s3_path as 'folder1/file.csv', returns (bucket, key) """
    # For this codebase, bucket is fixed, s3_path is the key
    return S3_BUCKET_NAME, s3_path.lstrip("/")

# --- CSV ---
def save_csv(df, path):
    if USE_S3:
        bucket, key = _parse_s3_path(str(path))
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8")

def load_csv(path):
    if USE_S3:
        bucket, key = _parse_s3_path(str(path))
        obj = get_s3_client().get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-8")
    else:
        return pd.read_csv(path, encoding="utf-8")

def list_files(directory, pattern="*"):
    """List files in a directory (local or S3)."""
    if USE_S3:
        bucket, prefix = _parse_s3_path(str(directory))
        s3 = get_s3_client()
        paginator = s3.get_paginator('list_objects_v2')
        file_list = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj["Key"].endswith(pattern.replace("*", "")):
                    file_list.append(obj["Key"])
        return file_list
    else:
        return list(Path(directory).glob(pattern))

# --- Excel ---
def save_excel(df, path):
    if USE_S3:
        bucket, key = _parse_s3_path(str(path))
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine="openpyxl")
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=excel_buffer.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(path, index=False, engine="openpyxl")

def load_excel(path, usecols=None):
    if USE_S3:
        bucket, key = _parse_s3_path(str(path))
        obj = get_s3_client().get_object(Bucket=bucket, Key=key)
        return pd.read_excel(io.BytesIO(obj["Body"].read()), usecols=usecols)
    else:
        return pd.read_excel(path, usecols=usecols)

# --- Pickle ---
def save_pickle(obj, path):
    import pickle
    if USE_S3:
        bucket, key = _parse_s3_path(str(path))
        buf = io.BytesIO()
        pickle.dump(obj, buf)
        buf.seek(0)
        get_s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(obj, f)

def load_pickle(path):
    import pickle
    if USE_S3:
        bucket, key = _parse_s3_path(str(path))
        obj = get_s3_client().get_object(Bucket=bucket, Key=key)
        return pickle.load(io.BytesIO(obj["Body"].read()))
    else:
        with open(path, "rb") as f:
            return pickle.load(f)

# --- Delete all files in a directory ---
def delete_all(directory):
    if USE_S3:
        bucket, prefix = _parse_s3_path(str(directory))
        s3 = get_s3_client()
        objects_to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        delete_keys = [{"Key": obj["Key"]} for obj in objects_to_delete.get("Contents", [])]
        if delete_keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})
    else:
        p = Path(directory)
        if not p.exists(): return
        for f in p.rglob("*"):
            try:
                if f.is_file(): f.unlink()
                elif f.is_dir(): f.rmdir()
            except Exception: pass
