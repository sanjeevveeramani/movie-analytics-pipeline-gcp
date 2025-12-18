import os
import json
import datetime
import requests
from flask import Flask, jsonify, request

from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import firestore

app = Flask(__name__)

# Support both env naming styles
STATE_COLLECTION = os.environ.get("STATE_COLLECTION") or os.environ.get("CURSOR_COLLECTION") or "cursors"
STATE_DOC_ID = os.environ.get("STATE_DOC_ID") or os.environ.get("CURSOR_DOC") or "tmdb_discover"


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


def tmdb_discover_movies(tmdb_credential: str, start_page: int, pages: int, auth_mode: str):
    """
    TMDB discover returns ~20 results per page.
    For ~2000 rows per run -> pages ~100.
    auth_mode:
      - "v3" -> uses ?api_key=
      - "v4" -> uses Authorization: Bearer <token>
    """
    base_url = "https://api.themoviedb.org/3/discover/movie"

    all_rows = []
    now_ts = datetime.datetime.utcnow().isoformat()
    batch_date = datetime.date.today().isoformat()

    end_page = start_page + pages - 1
    last_success_page = start_page - 1
    last_total_pages = None

    for page in range(start_page, end_page + 1):
        params = {
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page,
        }

        headers = {"accept": "application/json"}

        if auth_mode == "v3":
            params["api_key"] = tmdb_credential
        else:
            headers["Authorization"] = f"Bearer {tmdb_credential}"

        r = requests.get(base_url, headers=headers, params=params, timeout=30)

        # If unauthorized, stop and raise a useful error
        if r.status_code == 401:
            raise RuntimeError("TMDB returned 401 Unauthorized. Your TMDB key/token is invalid or wrong auth mode.")

        r.raise_for_status()
        data = r.json()

        total_pages = int(data.get("total_pages", 0) or 0)
        last_total_pages = total_pages if total_pages else last_total_pages

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

    return all_rows, last_success_page, last_total_pages


@app.get("/")
def health():
    return jsonify({"status": "ok", "service": "movie-api-ingestor"})


@app.get("/state")
def read_state():
    project_id = os.environ["PROJECT_ID"]
    db = firestore.Client(project=project_id)
    state = get_firestore_state(db)
    return jsonify({"collection": STATE_COLLECTION, "doc": STATE_DOC_ID, "state": state})


@app.get("/run")
def run_ingestion():
    try:
        project_id = os.environ["PROJECT_ID"]
        bucket_name = os.environ["BUCKET_NAME"]
        secret_name = os.environ.get("SECRET_NAME", "tmdb-api-key")

        # 100 pages ~ 2000 rows (20/page)
        default_pages = int(os.environ.get("PAGES_PER_RUN", "100"))

        db = firestore.Client(project=project_id)
        state = get_firestore_state(db)
        next_page = int(state.get("next_page", 1))

        start_page = int(request.args.get("start_page", next_page))
        pages = int(request.args.get("pages", default_pages))

        tmdb_credential = get_secret(project_id, secret_name)

        # Decide auth mode:
        # If token looks long (v4 tokens are long), use v4, else v3
        auth_mode = "v4" if len(tmdb_credential) > 40 else "v3"

        rows, last_success_page, total_pages = tmdb_discover_movies(
            tmdb_credential, start_page=start_page, pages=pages, auth_mode=auth_mode
        )

        batch_date = datetime.date.today().isoformat()
        ts_compact = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        gcs_path = (
            f"raw/api/batch_date={batch_date}/"
            f"movies_pages_{start_page}_to_{last_success_page}_{ts_compact}.jsonl"
        )

        jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
        upload_to_gcs(bucket_name, gcs_path, jsonl)

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
            tmdb_auth_mode=auth_mode,
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
                "tmdb_auth_mode": auth_mode,
            }
        )

    except Exception as e:
        # Return JSON error instead of blank Internal Server Error page
        return jsonify({"message": "ingestion_failed", "error": str(e)}), 500

