# TrackGraph backend (FastAPI).
#
# Sections
# - Config & env helpers
# - Data loading (local/S3) + preparation
# - Caching (dataset + response caches)
# - Spotify auth utilities (app token)
# - Aggregation helpers (bubbles + historical)
# - Image/metadata cache (local/S3)
# - API routes
# - Observability wiring

from __future__ import annotations
from typing import Literal, Optional
import time
import json
import io
from pathlib import Path
from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, Query, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import httpx
import requests
import threading

try:
    from backend import history
except ModuleNotFoundError:
    import history  # type: ignore

try:
    from backend import hydration_aggregation as analytics
except ModuleNotFoundError:
    import hydration_aggregation as analytics  # type: ignore

try:
    from backend import observability
except ModuleNotFoundError:
    import observability  # type: ignore

try:
    from backend.config import (
        AWS_REGION,
        DEMO_MODE,
        IMAGE_CACHE_S3_BUCKET,
        IMAGE_CACHE_S3_KEY,
        SPOTIFY_CLIENT_ID,
        SPOTIFY_CLIENT_SECRET,
    )
except ModuleNotFoundError:
    from config import (
        AWS_REGION,
        DEMO_MODE,
        IMAGE_CACHE_S3_BUCKET,
        IMAGE_CACHE_S3_KEY,
        SPOTIFY_CLIENT_ID,
        SPOTIFY_CLIENT_SECRET,
    )

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover - optional dependency for local dev
    boto3 = None
    BotoCoreError = ClientError = Exception

# Used to fetch the app token from Spotify
_token_cache = {"access_token": None, "expires_at": 0.0}

async def get_app_token() -> str:
    # Return app access token (client-credentials), cached in memory.
    # Reuse if not expired
    if _token_cache["access_token"] and _token_cache["expires_at"] - 60 > time.time():
        return _token_cache["access_token"]
    if not (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET):
        raise HTTPException(503, "Spotify credentials not configured")
    # Fetch new token
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
        )
        if r.status_code != 200:
            raise HTTPException(502, f"Spotify token error: {r.text}")
        data = r.json()
        _token_cache["access_token"] = data["access_token"]
        _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
        return _token_cache["access_token"]


_resp_cache_bubbles: dict = {}
_resp_cache_summary: dict = {}
_resp_cache_historical: dict = {}

# Simple in-memory response caches keyed by dataset version + filters
def _clear_response_caches():
    # Drop all per-response caches.
    _resp_cache_bubbles.clear()
    _resp_cache_summary.clear()
    _resp_cache_historical.clear()


def _on_dataset_change(_version: int) -> None:
    _clear_response_caches()


history.register_on_change(_on_dataset_change)


def get_app_token_sync() -> str:
    # Sync variant of app token retrieval (for requests usage).
    now = time.time()
    if _token_cache["access_token"] and _token_cache["expires_at"] - 60 > now:
        return _token_cache["access_token"]
    if not (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET):
        raise HTTPException(503, "Spotify credentials not configured")
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
        timeout=15,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"Spotify token error: {r.text}")
    data = r.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600)
    return _token_cache["access_token"]



# === Spotify batch helpers ===

def _chunks(seq, n):
    # Yield fixed-size chunks from `seq`.
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


@lru_cache(maxsize=4000)
def batch_tracks(ids_tuple):
    # Return minimal track objects for ids (50 per request).
    ids = list(ids_tuple)
    out = []
    for chunk in _chunks(ids, 50):
        # Not in cache, fetch from API
        token = get_app_token_sync()
        headers = {"Authorization": f"Bearer {token}"}
        params = {"ids": ",".join(chunk)}
        data = requests.get("https://api.spotify.com/v1/tracks", headers=headers, params=params)
        if data.status_code != 200:
            raise HTTPException(502, f"Spotify search error: {data.text}")
        data = data.json()
        out.extend(data.get("tracks", []))
    return out

@lru_cache(maxsize=4000)
def batch_artists(ids_tuple):
    # Return minimal artist objects for ids (50 per request).
    ids = list(ids_tuple)
    out = []
    for chunk in _chunks(ids, 50):
        # Not in cache, fetch from API
        token = get_app_token_sync()
        headers = {"Authorization": f"Bearer {token}"}
        params = {"ids": ",".join(chunk)}
        data = requests.get("https://api.spotify.com/v1/artists", headers=headers, params=params)
        if data.status_code != 200:
            raise HTTPException(502, f"Spotify search error: {data.text}")
        data = data.json()
        out.extend(data.get("artists", []))
    return out


from typing import Dict

# Persistent image/metadata cache (local file + optional S3)
class SpotifyImageCache:
    # JSON cache for track/artist metadata and images.
    #
    # - Reads from S3 if configured, else local file next to main.py.
    # - Writes both locally and to S3 (best-effort; errors are swallowed).
    def __init__(self, cache_file: str = "static_data.json"):
        base = Path(__file__).resolve().parent
        self.cache_file = (base / cache_file)
        self.cache: Dict[str, Optional[str]] = {}
        self.s3_bucket = IMAGE_CACHE_S3_BUCKET
        self.s3_key = IMAGE_CACHE_S3_KEY
        self.aws_region = AWS_REGION
        self._load_cache()

    def _load_cache_local(self) -> Dict[str, Optional[str]]:
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _load_cache_s3(self) -> Optional[Dict[str, Optional[str]]]:
        if not (self.s3_bucket and self.s3_key and boto3):
            return None
        try:
            s3 = boto3.client("s3", region_name=self.aws_region) if self.aws_region else boto3.client("s3")
            obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            body = obj["Body"].read().decode("utf-8")
            return json.loads(body)
        except Exception:
            # Fall back to local if S3 read fails
            return None

    def _load_cache(self):
        # Load cache (prefer S3; mirror to local file when available).
        s3_cache = self._load_cache_s3()
        if isinstance(s3_cache, dict) and s3_cache:
            self.cache = s3_cache
            # Also persist a local copy for quick subsequent loads
            try:
                with open(self.cache_file, 'w') as f:
                    json.dump(self.cache, f, indent=2)
            except IOError:
                pass
            return
        self.cache = self._load_cache_local()

    def save_cache(self):
        # Persist cache locally and to S3 (if configured).
        # Save locally
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except IOError:
            pass
        # Save to S3 if configured
        if self.s3_bucket and self.s3_key and boto3:
            try:
                s3 = boto3.client("s3", region_name=self.aws_region) if self.aws_region else boto3.client("s3")
                s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=self.s3_key,
                    Body=json.dumps(self.cache).encode("utf-8"),
                    ContentType="application/json"
                )
            except Exception:
                # Do not crash the request path if S3 write fails
                pass
    
    def get_cached_image(self, entity_type: str, name: str) -> Optional[str]:
        key = f"{entity_type}:{name}"
        return self.cache.get(key)
    
    def get_cached_key(self, name: str) -> Optional[str]:
        key = f"{name}"
        return self.cache.get(key)
    
    def cache_image(self, name: str, url: Optional[str]):
        key = f"{name}"
        self.cache[key] = url

image_cache = SpotifyImageCache()

# === App + CORS ===
app = FastAPI(title="Spotify Viz API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
observability.setup(app, DEMO_MODE)

# Routes

@app.get("/api/summary")
def api_summary(start: Optional[str] = None, end: Optional[str] = None):
    # Totals for the selected window (ms, hours, plays).
    dataset_version = history.get_dataset_version()
    key = (dataset_version, start or "", end or "")
    cached = _resp_cache_summary.get(key)
    if cached is not None:
        return cached

    _t0 = time.perf_counter()
    df = history.filter_df(history.get_df(), start, end)
    _t_filter_end = time.perf_counter()
    total_ms = int(df["ms_played"].sum())
    out = {
        "total_ms": total_ms,
        "total_hours": history.to_hours(total_ms),
        "total_plays": int(df.shape[0]),
        "start": start,
        "end": end,
        "timings": {
            "filter_ms": round((_t_filter_end - _t0) * 1000, 1),
            "total_ms": round((time.perf_counter() - _t0) * 1000, 1),
        }
    }
    _resp_cache_summary[key] = out
    return out

@app.get("/api/bubbles")
def api_bubbles(
    group_by: Literal["artist","album"] = Query("artist"),
    start: Optional[str] = None,
    end: Optional[str] = None
):
    # Bubble items for artists/albums with image hydration and timings.
    dataset_version = history.get_dataset_version()
    key = (dataset_version, start or "", end or "", group_by)
    cached = _resp_cache_bubbles.get(key)
    if cached is not None:
        return cached

    _t0 = time.perf_counter()
    df = history.filter_df(history.get_df(), start, end)
    _t_filter_end = time.perf_counter()
    out = analytics.aggregates(
        df,
        group_by,
        image_cache=image_cache,
        batch_tracks_fn=batch_tracks,
        batch_artists_fn=batch_artists,
        filter_key=(start or "", end or ""),
    )
    timings = out.get("timings", {})
    timings = {
        **timings,
        "filter_ms": round((_t_filter_end - _t0) * 1000, 1),
        "total_ms": round((time.perf_counter() - _t0) * 1000, 1),
    }
    out["timings"] = timings
    _resp_cache_bubbles[key] = out

    # Background prewarm for the alternate bubbles view and historical for this timeframe
    def _prewarm_other_views():
        try:
            other_group = "album" if group_by == "artist" else "artist"
            other_key = (dataset_version, start or "", end or "", other_group)
            if _resp_cache_bubbles.get(other_key) is None:
                df2 = history.filter_df(history.get_df(), start, end)
                _resp_cache_bubbles[other_key] = analytics.aggregates(
                    df2,
                    other_group,
                    image_cache=image_cache,
                    batch_tracks_fn=batch_tracks,
                    batch_artists_fn=batch_artists,
                    filter_key=(start or "", end or ""),
                )

            hist_key = (dataset_version, start or "", end or "", int(200))
            if _resp_cache_historical.get(hist_key) is None:
                df3 = history.filter_df(history.get_df(), start, end)
                _resp_cache_historical[hist_key] = analytics.historical_data(
                    df3,
                    200,
                    filter_key=(start or "", end or ""),
                )
        except Exception:
            pass

    threading.Thread(target=_prewarm_other_views, daemon=True).start()
    return out

@app.get("/api/historical_data")
def api_historical_data(
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: Optional[int] = Query(200, ge=1, le=500),
):
    # Tabular top artists/albums/tracks (limited).
    dataset_version = history.get_dataset_version()
    key = (dataset_version, start or "", end or "", int(limit) if limit is not None else None)
    cached = _resp_cache_historical.get(key)
    if cached is not None:
        return cached

    _t0 = time.perf_counter()
    df = history.filter_df(history.get_df(), start, end)
    _t_filter_end = time.perf_counter()
    _t_build_start = time.perf_counter()
    out = analytics.historical_data(df, limit, filter_key=(start or "", end or ""))
    _t_build_end = time.perf_counter()
    out["timings"] = {
        "filter_ms": round((_t_filter_end - _t0) * 1000, 1),
        "build_ms": round((_t_build_end - _t_build_start) * 1000, 1),
        "total_ms": round((time.perf_counter() - _t0) * 1000, 1),
    }
    _resp_cache_historical[key] = out
    return out


@app.post("/api/upload_history")
async def api_upload_history(file: UploadFile = File(...)):
    # Accept CSV or JSON history upload and switch data source to uploaded.
    if not file.filename:
        raise HTTPException(400, "Please upload a CSV or JSON export from Spotify.")

    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Uploaded file is empty.")

    name = (file.filename or "").lower()
    ctype = (file.content_type or "").lower()

    # Parse CSV or JSON based on extension or content type
    try:
        if name.endswith(".csv") or "csv" in ctype:
            raw_df = pd.read_csv(io.BytesIO(contents))
        elif name.endswith(".json") or "json" in ctype:
            try:
                raw_df = pd.read_json(io.BytesIO(contents))
            except ValueError:
                # Fallback: load via json and normalise into DataFrame
                payload = json.loads(contents.decode("utf-8"))
                raw_df = pd.DataFrame(payload)
        else:
            raise HTTPException(400, "Unsupported file type. Please upload a CSV or JSON export from Spotify.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, f"Could not parse file: {exc}") from exc

    try:
        df = history.prepare_history_dataframe(raw_df)
    except Exception as exc:
        raise HTTPException(400, f"Invalid data format: {exc}") from exc

    history.set_df(df, "uploaded")
    return {"status": "ok", "rows": int(df.shape[0]), "source": "uploaded"}


@app.post("/api/use_default_history")
def api_use_default_history():
    # Switch data source to packaged/default dataset (idempotent).
    if history.get_data_source() == "default" and history.has_cached_df():
        cached = history.get_df()
        return {
            "status": "ok",
            "rows": int(cached.shape[0]),
            "source": "default",
            "idempotent": True,
        }
    df = history.load_df()
    history.set_df(df, "default")
    return {
        "status": "ok",
        "rows": int(df.shape[0]),
        "source": "default",
        "idempotent": False,
    }


@app.get("/api/tracks-batch")
async def tracks_batch(ids: str):
    all_ids = [i for i in ids.split(',') if i]
    unique_ids = list(dict.fromkeys(all_ids))

    missing_ids = []
    cached_objs = []
    for id in unique_ids:
        cached = image_cache.get_cached_key(id)  # could be dict (full object) or legacy string
        if isinstance(cached, dict) and cached.get("artists"):
            cached_objs.append(cached)           # <- HIT (no Spotify call)
        else:
            missing_ids.append(id)               # <- MISS (will fetch & upgrade)

    fetched = []
    if missing_ids and not DEMO_MODE and SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
        token = await get_app_token()
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=10) as client:
            for i in range(0, len(missing_ids), 50):
                chunk = ",".join(missing_ids[i:i+50])
                r = await client.get("https://api.spotify.com/v1/tracks",
                                     headers=headers, params={"ids": chunk})
                if r.status_code != 200:
                    raise HTTPException(502, f"Spotify search error: {r.text}")
                tracks = [t for t in r.json().get("tracks", []) if t]
                for t in tracks:
                    if not t or not t.get("id"):
                        continue
                    mini = {
                        "id": t["id"],
                        "name": t.get("name"),
                        "uri": t.get("uri"),
                        "external_urls": t.get("external_urls") or {},
                        "popularity": t.get("popularity"),
                        "artists": [
                            {
                                "id": a.get("id"),
                                "name": a.get("name"),
                                "uri": a.get("uri"),
                                "external_urls": a.get("external_urls") or {},
                            }
                            for a in (t.get("artists") or []) if a
                        ],
                        "album": {
                            "id": (t.get("album") or {}).get("id"),
                            "name": (t.get("album") or {}).get("name"),
                            "images": (t.get("album") or {}).get("images") or [],
                            "uri": (t.get("album") or {}).get("uri"),
                            "external_urls": (t.get("album") or {}).get("external_urls") or {},
                        }
                    }
                    fetched.append(mini)

        # upgrade cache with full objects so future runs are hits
        for t in fetched:
            image_cache.cache_image(t["id"], t)
        image_cache.save_cache()

    by_id = {t["id"]: t for t in (cached_objs + fetched)}
    final = [by_id[i] for i in unique_ids if i in by_id]
    return {"tracks": final}


@app.get("/api/artists-batch")
async def artists_batch(ids: str):
    # Batch artist metadata (cache-first; fetch & upgrade cache on miss).
    all_ids = [i for i in ids.split(',') if i]
    unique_ids = list(dict.fromkeys(all_ids))

    cached_objs, missing_ids = [], []
    for aid in unique_ids:
        cached = image_cache.get_cached_key(aid)  # dict or legacy string
        if isinstance(cached, dict) and cached.get("id"):
            cached_objs.append(cached)            # <- HIT
        else:
            missing_ids.append(aid)               # <- MISS

    fetched = []
    if missing_ids and not DEMO_MODE and SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
        token = await get_app_token()
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=10) as client:
            for i in range(0, len(missing_ids), 50):
                chunk = ",".join(missing_ids[i:i+50])
                r = await client.get("https://api.spotify.com/v1/artists",
                                     headers=headers, params={"ids": chunk})
                r.raise_for_status()
                artists = [a for a in r.json().get("artists", []) if a]
                for a in artists:
                    mini = {
                        "id": a.get("id"),
                        "name": a.get("name"),
                        "popularity": a.get("popularity"),
                        "genres": a.get("genres") or [],
                        "images": a.get("images") or [],
                        "uri": a.get("uri"),
                        "external_urls": a.get("external_urls") or {},
                    }
                    if mini["id"]:
                        fetched.append(mini)

        for a in fetched:
            image_cache.cache_image(a["id"], a)   # store full object
        image_cache.save_cache()

    by_id = {a["id"]: a for a in (cached_objs + fetched)}
    final = [by_id[i] for i in unique_ids if i in by_id]
    return {"artists": final}


@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/healthz")
def healthz():
    # Liveness check
    return "ok\n"
