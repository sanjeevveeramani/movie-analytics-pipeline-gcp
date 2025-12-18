import os
import json
import datetime
import requests
from flask import Flask, jsonify, request

from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import firestore

app = Flask(__name__)

STATE_COLLECTION = os.environ.get("STATE_COLLECTION", "ingestion_state")
STATE_DOC_ID = os.environ.get("STATE_DOC_ID", "tmdb_discover_movie")


def get_secret(project_id: str, secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()


def upload_to_gcs(bucket_name: str, gcs_path: str, content: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(content, content_type="application/x-ndjson")


def get_firestore_state(db: firestore.Client):
    doc_ref = db.collection(STATE_COLLECTION).document(STATE_DOC_ID)
    snap = doc_ref.get()
    if snap.exists:
        return snap.to_dict() or {}
    return {}


def set_firestore_state(db: firestore.Client, **fields):
    doc_ref = db.collection(STATE_COLLECTION).document(STATE_DOC_ID)
    fields["updated_at_utc"] = datetime.datetime.utcnow().isoformat()
    doc_ref.set(fields, merge=True)


def build_tmdb_auth(api_secret: str):
    """
    TMDB auth fix:
    - If secret looks like TMDB v4 token (JWT-ish), use Authorization: Bearer <token>
    - Else treat it as TMDB v3 API key and pass ?api_key=...
    """
    s = (api_secret or "").strip()

    # Heuristic: v4 token is usually a JWT starting with "eyJ" and has dots.
    is_v4_bearer = (s.startswith("eyJ") and "." in s)

    headers = {"accept": "application/json"}
    params = {}

    if is_v4_bearer:
        headers["Authorization"] = f"Bearer {s}"
    else:
        # Assume v3 API key
        params["api_key"] = s

    return headers, params, ("v4_bearer" if is_v4_bearer else "v3_api_key")


def tmdb_discover_movies(api_secret: str, start_page: int, pages: int):
    """
    TMDB discover returns ~20 results per page.
    For ~2000 rows per run -> pages ~100.
    """
    base_url = "https://api.themoviedb.org/3/discover/movie"
    headers, base_params, auth_mode = build_tmdb_auth(api_secret)

    all_rows = []
    now_ts = datetime.datetime.utcnow().isoformat()
    batch_date = datetime.date.today().isoformat()

    end_page = start_page + pages - 1
    last_success_page = start_page - 1
    last_total_pages = None

    for page in range(start_page, end_page + 1):
        params = {
            **base_params,
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page,
        }

        r = requests.get(base_url, headers=headers, params=params, timeout=30)

        # Make the error message clearer in logs if TMDB rejects auth
        if r.status_code == 401:
            raise requests.HTTPError(
                f"TMDB 401 Unauthorized. auth_mode={auth_mode}. "
                f"Check Secret Manager value for '{os.environ.get('SECRET_NAME','tmdb-api-key')}'. "
                f"Response: {r.text}",
                response=r,
            )

        r.raise_for_status()
        data = r.json()

        total_pages = int(data.get("total_pages", 0) or 0)
        last_total_pages = total_pages if total_pages else last_total_pages

        # If TMDB says there are no more pages, stop early
        if total_pages and page > total_pages:
            break

        results = data.get("results", [])
        for m in results:
            m["ingestion_timestamp"] = now_ts
            m["batch_date"] = batch_date
            m["source"] = "tmdb_discover_movie"
            m["pulled_page"] = page
            all_rows.append(m)

        last_success_page = page

    return all_rows, last_success_page, last_total_pages, auth_mode


@app.get("/")
def health():
    return jsonify({"status": "ok", "service": "movie-api-ingestor"})


@app.get("/state")
def read_state():
    project_id = os.environ["PROJECT_ID"]
    db = firestore.Client(project=project_id)
    state = get_firestore_state(db)
    return jsonify({"state": state})


@app.get("/run")
def run_ingestion():
    project_id = os.environ["PROJECT_ID"]
    bucket_name = os.environ["BUCKET_NAME"]
    secret_name = os.environ.get("SECRET_NAME", "tmdb-api-key")

    # pages per run: set this to 100 for ~2000 rows (20 per page)
    default_pages = int(os.environ.get("PAGES_PER_RUN", os.environ.get("PAGES", "100")))

    db = firestore.Client(project=project_id)
    state = get_firestore_state(db)

    # Cursor logic:
    # next_page is where we continue from. Default = 1
    next_page = int(state.get("next_page", 1))

    # Optional override (useful for debugging):
    # /run?start_page=1&pages=100
    start_page = int(request.args.get("start_page", next_page))
    pages = int(request.args.get("pages", default_pages))

    api_secret = get_secret(project_id, secret_name)

    rows, last_success_page, total_pages, auth_mode = tmdb_discover_movies(
        api_secret, start_page=start_page, pages=pages
    )

    batch_date = datetime.date.today().isoformat()
    ts_compact = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # Unique file per run (append-style)
    gcs_path = (
        f"raw/api/batch_date={batch_date}/"
        f"movies_pages_{start_page}_to_{last_success_page}_{ts_compact}.jsonl"
    )

    jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    upload_to_gcs(bucket_name, gcs_path, jsonl)

    # Update cursor:
    # If we reached the end, wrap back to 1 (optional but practical)
    new_next_page = last_success_page + 1
    if total_pages and new_next_page > total_pages:
        new_next_page = 1

    set_firestore_state(
        db,
        next_page=new_next_page,
        last_run_start_page=start_page,
        last_run_end_page=last_success_page,
        last_run_rows=len(rows),
        last_run_gcs_path=f"gs://{bucket_name}/{gcs_path}",
        total_pages=total_pages,
        auth_mode=auth_mode,
    )

    return jsonify(
        {
            "message": "ingestion_success",
            "rows": len(rows),
            "gcs_path": f"gs://{bucket_name}/{gcs_path}",
            "start_page": start_page,
            "end_page": last_success_page,
            "pages_requested": pages,
            "next_page_saved": new_next_page,
            "total_pages_seen": total_pages,
            "auth_mode": auth_mode,
        }
    )
