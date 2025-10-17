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
import os
from typing import Literal, Optional, Sequence
from datetime import datetime
import time
import json
import io
import tempfile
from pathlib import Path
from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, Query, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import httpx
import requests
import threading

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover - optional dependency for local dev
    boto3 = None
    BotoCoreError = ClientError = Exception

# === Env loading (local dev, optional) ===
try:
    # Load alongside backend/main.py if present
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)
    # Then load project-root .env if present
    load_dotenv(override=False)
except Exception:
    # Do not fail if python-dotenv is not installed
    pass

def _env_bool(name: str, default: bool = False) -> bool:
    # Parse a boolean env var with sane defaults.
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

# Spotify API credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
DEMO_MODE = _env_bool("DEMO_MODE", False)

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


# === Config ===
DATA_PATH = os.getenv(
    "SPOTIFY_HISTORY_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cleaned_streaming_history.csv"),
)

S3_BUCKET = os.getenv("SPOTIFY_HISTORY_S3_BUCKET")
S3_KEY = os.getenv("SPOTIFY_HISTORY_S3_KEY")
S3_ETAG_CACHE: dict[str, Optional[str]] = {"etag": None, "path": None}
AWS_REGION = os.getenv("AWS_REGION")

# Optional separate S3 location for the image/metadata cache (defaults to history bucket)
IMAGE_CACHE_S3_BUCKET = os.getenv("SPOTIFY_IMAGE_CACHE_S3_BUCKET") or S3_BUCKET
IMAGE_CACHE_S3_KEY = os.getenv("SPOTIFY_IMAGE_CACHE_S3_KEY", "static_data.json")

REQUIRED_COLUMNS = [
    "ts",
    "ms_played",
    "master_metadata_track_name",
    "master_metadata_album_artist_name",
    "master_metadata_album_album_name",
    "spotify_track_uri",
    "reason_start",
    "reason_end",
    "shuffle",
    "skipped",
]


def _prepare_history_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize history so downstream aggregations work consistently.
    if df is None:
        raise ValueError("No data provided")

    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts", "ms_played"])
    df = df.dropna(
        subset=["master_metadata_track_name", "master_metadata_album_artist_name"],
        how="all",
    )
    df["ms_played"] = pd.to_numeric(df["ms_played"], errors="coerce").fillna(0).astype("int64")

    for col in [
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "master_metadata_album_album_name",
    ]:
        df[col] = df[col].fillna("Unknown").astype(str).str.strip()

    df["date"] = df["ts"].dt.date
    df["track_id"] = df["spotify_track_uri"].astype(str).str.replace(
        "spotify:track:", "",
        regex=False,
    )

    return df

def _resolve_history_path() -> str:
    # Return local path to history, optionally downloading from S3 when set.
    if S3_BUCKET and S3_KEY:
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required to load streaming history from S3. Install boto3 or unset "
                "SPOTIFY_HISTORY_S3_BUCKET/SPOTIFY_HISTORY_S3_KEY."
            )
        s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
        try:
            meta = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
            etag = meta.get("ETag")
        except ClientError as exc:  # pragma: no cover - external dependency
            raise HTTPException(500, f"Unable to access S3 history object: {exc}") from exc

        if S3_ETAG_CACHE.get("etag") == etag and S3_ETAG_CACHE.get("path"):
            return S3_ETAG_CACHE["path"]

        suffix = Path(S3_KEY).suffix or ".csv"
        fd, tmp_path = tempfile.mkstemp(prefix="spotify_history_", suffix=suffix)
        os.close(fd)
        try:
            s3.download_file(S3_BUCKET, S3_KEY, tmp_path)
        except (ClientError, BotoCoreError) as exc:  # pragma: no cover - external dependency
            raise HTTPException(500, f"Failed to download history from S3: {exc}") from exc

        S3_ETAG_CACHE.update({"etag": etag, "path": tmp_path})
        return tmp_path

    if os.path.exists(DATA_PATH):
        return DATA_PATH

    static_json = Path(__file__).with_name("static_data.json")
    if static_json.exists():
        return static_json

    raise FileNotFoundError(
        "Streaming history not found. Set SPOTIFY_HISTORY_PATH or configure S3."
    )


def load_df() -> pd.DataFrame:
    # Load history as DataFrame; merge optional packaged sample if present.
    source_path = _resolve_history_path()
    if str(source_path).endswith(".json"):
        with open(source_path, "r") as fh:
            data = json.load(fh)
        df = pd.DataFrame(data)
    else:
        df = pd.read_csv(source_path)

    return _prepare_history_dataframe(df)

_df_cache: Optional[pd.DataFrame] = None
_dataset_version: int = 0
_data_source: str = "unknown"  # one of: unknown, default, uploaded

# Simple in-memory response caches keyed by dataset version + filters
_resp_cache_bubbles: dict = {}
_resp_cache_summary: dict = {}
_resp_cache_historical: dict = {}

def _clear_response_caches():
    # Drop all per-response caches.
    _resp_cache_bubbles.clear()
    _resp_cache_summary.clear()
    _resp_cache_historical.clear()

def _bump_dataset_version():
    global _dataset_version, _build_dicts_cache
    _dataset_version += 1
    _build_dicts_cache = {"version": None, "value": None}
    _clear_response_caches()

def get_df() -> pd.DataFrame:
    # Get cached DataFrame; lazy-load on first access and tag data source.
    global _df_cache, _data_source
    if _df_cache is None:
        _df_cache = load_df()
        _bump_dataset_version()
        if _data_source == "unknown":
            _data_source = "default"
    return _df_cache

def filter_df(df: pd.DataFrame, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    # Filter by ISO date range [start, end).
    m = pd.Series(True, index=df.index)
    if start:
        try:
            start_dt = pd.to_datetime(start, utc=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'start' date format. Use ISO like 2021-01-01.")
        m &= df["ts"] >= start_dt
    if end:
        try:
            end_dt = pd.to_datetime(end, utc=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'end' date format. Use ISO like 2021-12-31.")
        m &= df["ts"] < end_dt
    return df[m]

def to_hours(ms: float) -> float:
    # Convert milliseconds -> hours (rounded).
    return round(float(ms) / 3_600_000, 3)

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



def _unique_non_null(values) -> list[str]:
    # Return unique, truthy values preserving order.
    unique = []
    for val in pd.unique(values):
        if pd.notna(val) and val:
            unique.append(str(val))
    return unique

# === Spotify batch helpers ===

def _chunks(seq, n):
    # Yield fixed-size chunks from `seq`.
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def _hydrate_bubble_images(
    items: list[dict],
    group_by: Literal["artist", "album"],
    artist_album_to_id: dict,
    track_artist_to_album: dict,
    stats: Optional[dict] = None,
):
    # Attach best-effort `image_url` to items using cache -> Spotify -> cache.
    #
    # Strategy
    # - Pick a representative track id per item (prefer precomputed `ids`).
    # - Resolve missing track metadata via batch API; persist in cache.
    # - For artists, resolve artist images (cache-first; batch fetch on miss).
    if not items:
        return

    item_by_id = {item["id"]: item for item in items if item.get("id")}
    representative_tracks = {}

    for item in items:
        rep = None
        ids = item.get("ids") or []
        if ids:
            rep = next((track_id for track_id in ids if track_id), None)

        if not rep:
            if group_by == "album":
                artist_name = item.get("artist")
                if artist_name:
                    rep = artist_album_to_id.get(f"{artist_name}::{item['label']}")
            else:  # artist view
                for top in item.get("top_tracks") or []:
                    track_name = top.get("name") or top.get("master_metadata_track_name")
                    if not track_name:
                        continue
                    album_name = track_artist_to_album.get(f"{track_name}::{item['label']}")
                    if not album_name:
                        continue
                    rep = artist_album_to_id.get(f"{item['label']}::{album_name}")
                    if rep:
                        break

        if rep:
            representative_tracks[item["id"]] = rep

    if not representative_tracks:
        return

    # First resolve tracks from the persistent cache.
    track_by_id = {}
    missing_track_ids = []
    for track_id in sorted({tid for tid in representative_tracks.values() if tid}):
        cached = image_cache.get_cached_key(track_id)
        if isinstance(cached, dict) and cached.get("album"):
            track_by_id[track_id] = cached
            if isinstance(stats, dict):
                stats["tracks_cache_hits"] = stats.get("tracks_cache_hits", 0) + 1
        else:
            missing_track_ids.append(track_id)
            if isinstance(stats, dict):
                stats["tracks_cache_misses"] = stats.get("tracks_cache_misses", 0) + 1

    # Fetch any remaining tracks from Spotify (LRU cached) and refresh the persistent cache.
    fetched_any = False
    if missing_track_ids:
        try:
            track_payloads = batch_tracks(tuple(missing_track_ids))
        except HTTPException:
            track_payloads = []
        if isinstance(stats, dict):
            stats["tracks_fetched"] = stats.get("tracks_fetched", 0) + len([t for t in track_payloads if t])
        for t in track_payloads:
            if not t or not t.get("id"):
                continue
            track_by_id[t["id"]] = t
            image_cache.cache_image(t["id"], t)
            fetched_any = True
        if fetched_any:
            image_cache.save_cache()

    artist_ids = set()

    for item_id, track_id in representative_tracks.items():
        item = item_by_id.get(item_id)
        if not item:
            continue
        item["representative_track_id"] = track_id
        track = track_by_id.get(track_id)
        if not track:
            continue

        album = track.get("album") or {}
        album_images = album.get("images") or []
        if album_images and not item.get("image_url"):
            item["image_url"] = album_images[0].get("url")

        if group_by == "artist":
            chosen_artist = None
            for artist in track.get("artists") or []:
                if not artist:
                    continue
                name = artist.get("name")
                if name and name.lower() == item["label"].lower():
                    chosen_artist = artist
                    break
            if not chosen_artist and track.get("artists"):
                chosen_artist = track["artists"][0]

            if chosen_artist and chosen_artist.get("id"):
                artist_ids.add(chosen_artist["id"])
                item["_artist_id"] = chosen_artist["id"]

    # Resolve artist images from cache, falling back to Spotify when needed.
    if group_by == "artist" and artist_ids:
        artist_by_id = {}
        missing_artist_ids = []
        for aid in sorted(artist_ids):
            cached = image_cache.get_cached_key(aid)
            if isinstance(cached, dict) and cached.get("images"):
                artist_by_id[aid] = cached
                if isinstance(stats, dict):
                    stats["artists_cache_hits"] = stats.get("artists_cache_hits", 0) + 1
            else:
                missing_artist_ids.append(aid)
                if isinstance(stats, dict):
                    stats["artists_cache_misses"] = stats.get("artists_cache_misses", 0) + 1

        fetched_any_artists = False
        if missing_artist_ids:
            try:
                artist_payloads = batch_artists(tuple(missing_artist_ids))
            except HTTPException:
                artist_payloads = []
            if isinstance(stats, dict):
                stats["artists_fetched"] = stats.get("artists_fetched", 0) + len([a for a in artist_payloads if a])
            for a in artist_payloads:
                if not a or not a.get("id"):
                    continue
                artist_by_id[a["id"]] = a
                image_cache.cache_image(a["id"], a)
                fetched_any_artists = True
            if fetched_any_artists:
                image_cache.save_cache()

        for item in items:
            artist_id = item.pop("_artist_id", None)
            if not artist_id:
                continue
            artist = artist_by_id.get(artist_id)
            if not artist:
                continue
            images = artist.get("images") or []
            if images:
                item["image_url"] = images[0].get("url")

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


# Derived dictionaries (cached per dataset+filters)
_build_dicts_cache_map: dict = {}

def build_dicts(df: pd.DataFrame, filter_key: Optional[tuple] = None):
    # Build and cache mappings used across endpoints.
    #
    # Returns
    # - artist_album_to_id: "Artist::Album" -> track_id
    # - id_to_artist_album: track_id -> "Artist::Album"
    # - track_artist_to_album: "Track::Artist" -> album
    key = (_dataset_version, filter_key)
    hit = _build_dicts_cache_map.get(key)
    if hit is not None:
        return hit
    # Build the artist, album -> id dict
    historical_albums = (
        df.groupby(["master_metadata_album_artist_name","master_metadata_album_album_name"], dropna=False)
          .agg(ms_played=("ms_played","sum"),
               plays=("ts","count"),
               distinct_tracks=("master_metadata_track_name","nunique"),
               ids=("track_id", "first"))
          .reset_index()
    )
    aa_to_id_tuple = (
        historical_albums
        .set_index(['master_metadata_album_artist_name', 'master_metadata_album_album_name'])['ids']
        .to_dict()
    )
    # Id -> (artist, album) dict
    id_to_artist_album = {v: k for k, v in aa_to_id_tuple.items()}

    # (track, artist) -> album

    ta_to_album_tuple = (
        df.set_index(['master_metadata_track_name','master_metadata_album_artist_name'])['master_metadata_album_album_name']
        .to_dict()
    )

    # Map "Artist::Album" to a representative track id.
    artist_album_to_id = {
        f"{artist}::{album}": track_id
        for (artist, album), track_id in aa_to_id_tuple.items()
    }

    # Map track id back to "Artist::Album".
    id_to_artist_album = {
        track_id: f"{artist}::{album}"
        for (artist, album), track_id in aa_to_id_tuple.items()
    }

    # Map "Track::Artist" to the album name.
    track_artist_to_album = {
        f"{track}::{artist}": album
        for (track, artist), album in ta_to_album_tuple.items()
    }

    value = (artist_album_to_id, id_to_artist_album, track_artist_to_album)
    _build_dicts_cache_map[key] = value
    return value


# Aggregations

def aggregates(df: pd.DataFrame, group_by: Literal["artist","album"], filter_key: Optional[tuple] = None) -> dict:
    # Compute bubble items for artist/album groups and hydrate images.
    _t0 = time.perf_counter()
    if group_by == "artist":
        key_col = "master_metadata_album_artist_name"
    else:
        key_col = "master_metadata_album_album_name"
    
    _t_dicts_start = time.perf_counter()
    aa_id, id_aa, ta_a = build_dicts(df, filter_key)
    _t_dicts_end = time.perf_counter()
    # Add a boolean column if an artist has been played for more than 1 hour
    artist_playtime = df.groupby("master_metadata_album_artist_name")["ms_played"].sum()
    artists_over_1hr = artist_playtime[artist_playtime > 3_600_000].index

    album_playtime = df.groupby("master_metadata_album_album_name")["ms_played"].sum()
    albums_over_1hr = album_playtime[album_playtime > 3_600_000].index

    total_ms = int(df["ms_played"].sum())
    # Group by entity but also keep track of the artist for albums
    _t_group_start = time.perf_counter()
    if key_col == "master_metadata_album_artist_name":
        g = (
            df.groupby(key_col, dropna=False)
              .agg(
                  ms_total=("ms_played", "sum"),
                  plays=("ts", "count"),
                  distinct_tracks=("master_metadata_track_name", "nunique"),
                  ids=("track_id", lambda x: _unique_non_null(x)),
              )
              .sort_values("ms_total", ascending=False)
              .reset_index()
        )
        g["artist_over_1hr"] = g[key_col].isin(artists_over_1hr)
        limit = int(g["artist_over_1hr"].sum())
    else:
        g = (
            df.groupby([key_col, "master_metadata_album_artist_name"], dropna=False)
              .agg(
                  ms_total=("ms_played", "sum"),
                  plays=("ts", "count"),
                  distinct_tracks=("master_metadata_track_name", "nunique"),
                  ids=("track_id", lambda x: _unique_non_null(x)),
              )
              .sort_values("ms_total", ascending=False)
              .reset_index()
        )
        g["albums_over_1hr"] = g[key_col].isin(albums_over_1hr)
        limit = int(g["albums_over_1hr"].sum())
    _t_group_end = time.perf_counter()

    max_items = min(len(g), 160)
    if not limit:
        limit = min(max_items, 120)
    limit = min(limit, max_items)
    g = g[g["ms_total"] > 0]
    if limit:
        g = g.head(limit)
    # Top tracks per entity
    tt = (
        df.groupby([key_col, "master_metadata_track_name"], dropna=False)
          .agg(track_ms=("ms_played","sum"),
               track_plays=("ts","count"))
          .reset_index()
    )

    items = []
    for row in g.reset_index().to_dict(orient="records"):
        name = row[key_col]
        ms_total = int(row["ms_total"])
        sub = tt[tt[key_col] == name].sort_values("track_ms", ascending=False).head(5)
        top_tracks = [
            {
                "name": r["master_metadata_track_name"],
                "ms": int(r["track_ms"]),
                "hours": to_hours(r["track_ms"]),
                "plays": int(r["track_plays"]),
            }
            for _, r in sub.iterrows()
        ]
        pct = (ms_total / total_ms) if total_ms else 0.0

        item_ids = row.get("ids") or []
        item = {
            "id": name,
            "label": name,
            "value_ms": ms_total,
            "value_hours": to_hours(ms_total),
            "value_pct": pct,
            "plays": int(row["plays"]),
            "distinct_tracks": int(row["distinct_tracks"]),
            "top_tracks": top_tracks,
            "ids": item_ids,
        }

        if group_by == "album":
            item["artist"] = row.get("master_metadata_album_artist_name")
        items.append(item)

    hydrate_stats = {
        "tracks_cache_hits": 0,
        "tracks_cache_misses": 0,
        "tracks_fetched": 0,
        "artists_cache_hits": 0,
        "artists_cache_misses": 0,
        "artists_fetched": 0,
    }
    _t_hydrate_start = time.perf_counter()
    _hydrate_bubble_images(items, group_by, aa_id, ta_a, stats=hydrate_stats)
    _t_hydrate_end = time.perf_counter()

    _t1 = time.perf_counter()
    return {
        "group_by": group_by,
        "total_ms": total_ms,
        "total_hours": to_hours(total_ms),
        "total_plays": int(df.shape[0]),
        "items": items,
        "artist_album_to_id": aa_id,
        "id_to_artist_album": id_aa,
        "track_artist_to_album": ta_a,
        "timings": {
            "dicts_ms": round((_t_dicts_end - _t_dicts_start) * 1000, 1),
            "groupby_ms": round((_t_group_end - _t_group_start) * 1000, 1),
            "hydrate_ms": round((_t_hydrate_end - _t_hydrate_start) * 1000, 1),
            "aggregates_ms": round((_t1 - _t0) * 1000, 1),
            "hydrate_stats": hydrate_stats,
        }
    }


def historical_data(df: pd.DataFrame, limit: Optional[int] = None, filter_key: Optional[tuple] = None) -> dict:
    # Top artists/albums/tracks with simple counts; limit applied per table.
    aa_id, id_aa, ta_a = build_dicts(df, filter_key)
    # Return three dataframes: artists, albums, tracks
    historical_artists = (
        df.groupby(["master_metadata_album_artist_name"], dropna=False)
          .agg(ms_played=("ms_played","sum"),
               plays=("ts","count"),
               distinct_tracks=("master_metadata_track_name","nunique"))
          .reset_index()
    ).sort_values("plays", ascending=False)

    historical_albums = (
        df.groupby(["master_metadata_album_album_name"], dropna=False)
          .agg(ms_played=("ms_played","sum"),
               plays=("ts","count"),
               distinct_tracks=("master_metadata_track_name","nunique"))
          .reset_index()
    ).sort_values("plays", ascending=False)

    # Keep the id of the track as well
    historical_tracks = (
            df.groupby(["master_metadata_track_name","spotify_track_uri"], dropna=False)
              .agg(ms_played=("ms_played","sum"),
                   plays=("ts","count"))
              .reset_index()
        ).sort_values("plays", ascending=False)
    
    historical_tracks["spotify_track_uri"] = historical_tracks["spotify_track_uri"].str.replace("spotify:track:", "", regex=False)

    # rename spotify_track_uri to track_id
    historical_tracks.rename(columns={"spotify_track_uri": "track_id"}, inplace=True)
    
    if limit:
        limit = max(1, min(int(limit), 500))
        historical_artists = historical_artists.head(limit)
        historical_albums = historical_albums.head(limit)
        historical_tracks = historical_tracks.head(limit)
    
    return {
        "artists": historical_artists.to_dict(orient="records"),
        "albums": historical_albums.to_dict(orient="records"),
        "tracks": historical_tracks.to_dict(orient="records"),
        "artist_album_to_id": aa_id,
        "id_to_artist_album": id_aa,
        "track_artist_to_album": ta_a
    }

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


def _is_low_value_span_value(value: object) -> bool:
    # Return True if the value clearly references low-value routes (/healthz, /metrics).
    if value is None:
        return False
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", "ignore")
        except Exception:
            return False
    text = str(value).strip().lower()
    if not text:
        return False
    suffixes = ("/healthz", "/metrics", "/metrics/")
    if any(text == suffix for suffix in suffixes):
        return True
    if any(text.endswith(suffix) for suffix in suffixes):
        return True
    prefixes = (
        "get /healthz",
        "head /healthz",
        "get /metrics",
        "head /metrics",
    )
    if any(text.startswith(prefix) for prefix in prefixes):
        return True
    return False


def _span_is_low_value(span) -> bool:
    # Inspect name + common HTTP attributes to detect low-value spans (health/metrics).
    try:
        attributes = getattr(span, "attributes", {}) or {}
    except Exception:
        attributes = {}

    candidates = [
        getattr(span, "name", ""),
        attributes.get("http.target"),
        attributes.get("http.route"),
        attributes.get("http.path"),
        attributes.get("http.request.uri"),
        attributes.get("url.path"),
        attributes.get("http.url"),
        attributes.get("url.full"),
    ]
    return any(_is_low_value_span_value(value) for value in candidates)


# Tracing: OpenTelemetry (select OTLP HTTP/GRPC via env)
try:
    if not getattr(app, "_otel_tracing_initialized", False) and not DEMO_MODE:
        import os
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor,
            SpanExportResult,
            SpanExporter,
        )
        # AWS X-Ray ID/propagation
        from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
        from opentelemetry.propagators.aws import AwsXRayPropagator
        from opentelemetry.propagate import set_global_textmap
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as OTLPSpanExporterGRPC
        # HTTP exporter imported lazily if requested
        import logging


        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        class _LowValueFilterSpanExporter(SpanExporter):
            # Wrap another exporter and drop health/metrics spans before export.

            def __init__(self, delegate: SpanExporter):
                self._delegate = delegate

            def export(self, spans: Sequence):
                filtered = [span for span in spans if not _span_is_low_value(span)]
                if not filtered:
                    return SpanExportResult.SUCCESS
                return self._delegate.export(filtered)

            def shutdown(self) -> None:
                return self._delegate.shutdown()

            def force_flush(self, timeout_millis: int | None = None) -> bool:
                return self._delegate.force_flush(timeout_millis=timeout_millis)

        resource = Resource.create({
            "service.name": os.getenv("OTEL_SERVICE_NAME", "backend"),
            "service.namespace": "trackgraph",
        })

        provider = TracerProvider(resource=resource, id_generator=AwsXRayIdGenerator())
        trace.set_tracer_provider(provider)

        # Use AWS X-Ray propagation so traces stitch in X-Ray UI
        set_global_textmap(AwsXRayPropagator())

        # Decide protocol/endpoints from env
        proto = (
            os.getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            or os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
            or "grpc"
        ).strip().lower()
        traces_ep = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        generic_ep = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

        exporter = None
        chosen = None
        ep_used = None

        if proto.startswith("http"):
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as OTLPSpanExporterHTTP  # type: ignore
                if traces_ep:
                    ep_used = traces_ep
                elif generic_ep:
                    ep_used = generic_ep.rstrip("/") + "/v1/traces"
                else:
                    ep_used = "http://localhost:4318/v1/traces"
                exporter = OTLPSpanExporterHTTP(endpoint=ep_used)
                chosen = f"otlp-http:{ep_used}"
            except Exception as _http_exc:
                # HTTP exporter not available. If the configured endpoint is HTTP(S),
                # do NOT fall back to gRPC (would 404/464). Disable exporting instead.
                http_like = (traces_ep or generic_ep or "").startswith("http")
                if http_like:
                    try:
                        logging.getLogger("startup").warning(
                            "otel_exporter_unavailable",
                            extra={
                                "protocol": proto,
                                "endpoint": (traces_ep or generic_ep),
                                "reason": "missing otlp http exporter",
                            },
                        )
                    except Exception:
                        pass
                    exporter = None
                    chosen = "disabled"
                else:
                    proto = "grpc"

        if exporter is None and proto == "grpc":
            ep_used = traces_ep or generic_ep or "grpc://localhost:4317"
            insecure = ep_used.startswith(("grpc://", "http://"))
            exporter = OTLPSpanExporterGRPC(endpoint=ep_used, insecure=insecure)
            chosen = f"otlp-grpc:{ep_used}"

        try:
            logging.getLogger("startup").info(
                "otel_exporter",
                extra={"protocol": proto, "endpoint": ep_used, "chosen": chosen},
            )
        except Exception:
            pass

        if exporter is not None:
            exporter = _LowValueFilterSpanExporter(exporter)
            provider.add_span_processor(BatchSpanProcessor(exporter))
        else:
            try:
                logging.getLogger("startup").info(
                    "otel_exporter_disabled", extra={"protocol": proto, "endpoint": ep_used}
                )
            except Exception:
                pass

        # Auto-instrument (skip health/metrics to reduce noise)
        FastAPIInstrumentor().instrument_app(app, excluded_urls=r"^/(healthz|metrics)$")
        RequestsInstrumentor().instrument()

        setattr(app, "_otel_tracing_initialized", True)
except Exception:
    # Never break the app if tracing setup fails
    pass

# Observability: JSON logs + Prometheus /metrics
try:
    # Local import (same directory as main.py)
    from instrumentation import request_logger_middleware, make_metrics_asgi_app  # type: ignore

    # In demo mode, disable request logging middleware and metrics entirely.
    if not getattr(app, "_instrumentation_initialized", False):
        if not DEMO_MODE:
            app.middleware("http")(request_logger_middleware)
            # Use wrapped metrics app that logs incoming Host/X-Forwarded-* headers
            app.mount("/metrics", make_metrics_asgi_app())
        setattr(app, "_instrumentation_initialized", True)
except Exception:
    # Never break the app if instrumentation fails to import
    pass

# Routes

@app.get("/api/summary")
def api_summary(start: Optional[str] = None, end: Optional[str] = None):
    # Totals for the selected window (ms, hours, plays).
    key = (_dataset_version, start or "", end or "")
    cached = _resp_cache_summary.get(key)
    if cached is not None:
        return cached

    _t0 = time.perf_counter()
    df = filter_df(get_df(), start, end)
    _t_filter_end = time.perf_counter()
    total_ms = int(df["ms_played"].sum())
    out = {
        "total_ms": total_ms,
        "total_hours": to_hours(total_ms),
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
    key = (_dataset_version, start or "", end or "", group_by)
    cached = _resp_cache_bubbles.get(key)
    if cached is not None:
        return cached

    _t0 = time.perf_counter()
    df = filter_df(get_df(), start, end)
    _t_filter_end = time.perf_counter()
    out = aggregates(df, group_by, filter_key=(start or "", end or ""))
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
            other_key = (_dataset_version, start or "", end or "", other_group)
            if _resp_cache_bubbles.get(other_key) is None:
                df2 = filter_df(get_df(), start, end)
                _resp_cache_bubbles[other_key] = aggregates(df2, other_group, filter_key=(start or "", end or ""))

            hist_key = (_dataset_version, start or "", end or "", int(200))
            if _resp_cache_historical.get(hist_key) is None:
                df3 = filter_df(get_df(), start, end)
                _resp_cache_historical[hist_key] = historical_data(df3, 200, filter_key=(start or "", end or ""))
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
    key = (_dataset_version, start or "", end or "", int(limit) if limit is not None else None)
    cached = _resp_cache_historical.get(key)
    if cached is not None:
        return cached

    _t0 = time.perf_counter()
    df = filter_df(get_df(), start, end)
    _t_filter_end = time.perf_counter()
    _t_build_start = time.perf_counter()
    out = historical_data(df, limit, filter_key=(start or "", end or ""))
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
        df = _prepare_history_dataframe(raw_df)
    except Exception as exc:
        raise HTTPException(400, f"Invalid data format: {exc}") from exc

    global _df_cache, _data_source
    _df_cache = df
    _data_source = "uploaded"
    _bump_dataset_version()

    return {"status": "ok", "rows": int(df.shape[0]), "source": "uploaded"}


@app.post("/api/use_default_history")
def api_use_default_history():
    # Switch data source to packaged/default dataset (idempotent).
    global _df_cache, _data_source
    if _data_source == "default" and _df_cache is not None:
        return {"status": "ok", "rows": int(_df_cache.shape[0]), "source": "default", "idempotent": True}
    _df_cache = load_df()
    _data_source = "default"
    _bump_dataset_version()
    return {"status": "ok", "rows": int(_df_cache.shape[0]), "source": "default", "idempotent": False}


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
