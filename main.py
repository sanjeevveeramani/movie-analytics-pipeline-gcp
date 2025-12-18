import os
import json
import datetime
import requests
from flask import Flask, jsonify, request

from google.cloud import storage
from google.cloud import secretmanager

app = Flask(__name__)

def get_secret(project_id: str, secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def upload_to_gcs(bucket_name: str, gcs_path: str, content: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(content, content_type="application/x-ndjson")

def tmdb_discover_movies(api_key: str, start_page: int = 1, pages: int = 5):
    base_url = "https://api.themoviedb.org/3/discover/movie"
    headers = {"accept": "application/json"}
    all_rows = []

    end_page = start_page + pages - 1

    for page in range(start_page, end_page + 1):
        params = {
            "api_key": api_key,
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page
        }
        r = requests.get(base_url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        results = data.get("results", [])
        now_ts = datetime.datetime.utcnow().isoformat()
        batch_date = datetime.date.today().isoformat()

        for m in results:
            m["ingestion_timestamp"] = now_ts
            m["batch_date"] = batch_date
            m["source"] = "api"
            m["pulled_page"] = page  # helpful for debugging / auditing
            all_rows.append(m)

    return all_rows

@app.get("/")
def health():
    return jsonify({"status": "ok", "service": "movie-api-ingestor"})

@app.get("/run")
def run_ingestion():
    project_id = os.environ["PROJECT_ID"]
    bucket_name = os.environ["BUCKET_NAME"]
    secret_name = os.environ.get("SECRET_NAME", "tmdb-api-key")

    # defaults from env (if no query params passed)
    default_pages = int(os.environ.get("PAGES", "5"))
    default_start_page = int(os.environ.get("START_PAGE", "1"))

    # Option B: override via URL query params
    start_page = int(request.args.get("start_page", default_start_page))
    pages = int(request.args.get("pages", default_pages))

    api_key = get_secret(project_id, secret_name)
    rows = tmdb_discover_movies(api_key, start_page=start_page, pages=pages)

    batch_date = datetime.date.today().isoformat()

    # IMPORTANT: unique filename so it APPENDS (doesn't overwrite)
    gcs_path = f"raw/api/batch_date={batch_date}/movies_start={start_page}_pages={pages}.jsonl"

    jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    upload_to_gcs(bucket_name, gcs_path, jsonl)

    return jsonify({
        "message": "ingestion_success",
        "rows": len(rows),
        "gcs_path": f"gs://{bucket_name}/{gcs_path}",
        "start_page": start_page,
        "pages": pages
    })
