"""Streaming history loading and caching helpers."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from fastapi import HTTPException

from config import (
    AWS_REGION,
    DATA_PATH,
    REQUIRED_COLUMNS,
    S3_BUCKET,
    S3_ETAG_CACHE,
    S3_KEY,
)

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover - optional dependency for local dev
    boto3 = None
    BotoCoreError = ClientError = Exception

DatasetChangeCallback = Callable[[int], None]

_change_listeners: list[DatasetChangeCallback] = []

_df_cache: Optional[pd.DataFrame] = None
_dataset_version: int = 0
_data_source: str = "unknown"  # one of: unknown, default, uploaded


def register_on_change(callback: DatasetChangeCallback) -> None:
    """Register a callback invoked whenever the dataset version increments."""
    if callback not in _change_listeners:
        _change_listeners.append(callback)


def _notify_change(version: int) -> None:
    for listener in list(_change_listeners):
        try:
            listener(version)
        except Exception:
            # Swallow listener failures; observability/logging should happen externally.
            continue


def get_dataset_version() -> int:
    return _dataset_version


def _bump_dataset_version() -> int:
    global _dataset_version
    _dataset_version += 1
    _notify_change(_dataset_version)
    return _dataset_version


def get_data_source() -> str:
    return _data_source


def has_cached_df() -> bool:
    return _df_cache is not None


def prepare_history_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize history so downstream aggregations work consistently."""
    if df is None:
        raise ValueError("No data provided")

    out = df.copy()
    out.columns = [c.strip() for c in out.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in out.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out = out.dropna(subset=["ts", "ms_played"])
    out = out.dropna(
        subset=["master_metadata_track_name", "master_metadata_album_artist_name"],
        how="all",
    )
    out["ms_played"] = pd.to_numeric(out["ms_played"], errors="coerce").fillna(0).astype("int64")

    for col in [
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "master_metadata_album_album_name",
    ]:
        out[col] = out[col].fillna("Unknown").astype(str).str.strip()

    out["date"] = out["ts"].dt.date
    out["track_id"] = out["spotify_track_uri"].astype(str).str.replace(
        "spotify:track:", "",
        regex=False,
    )

    return out


def _resolve_history_path() -> str:
    """Return local path to history, optionally downloading from S3 when set."""
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
            cached_path = S3_ETAG_CACHE["path"]
            if cached_path and os.path.exists(cached_path):
                return cached_path

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
    """Load history as DataFrame; merge optional packaged sample if present."""
    source_path = _resolve_history_path()
    if str(source_path).endswith(".json"):
        with open(source_path, "r") as fh:
            data = json.load(fh)
        df = pd.DataFrame(data)
    else:
        df = pd.read_csv(source_path)

    return prepare_history_dataframe(df)


def get_df() -> pd.DataFrame:
    """Return cached DataFrame; lazy-load on first access."""
    global _df_cache, _data_source
    if _df_cache is None:
        _df_cache = load_df()
        if _data_source == "unknown":
            _data_source = "default"
        _bump_dataset_version()
    return _df_cache


def set_df(df: pd.DataFrame, source: str) -> None:
    """Replace the cached DataFrame and update the current data source flag."""
    global _df_cache, _data_source
    _df_cache = df
    _data_source = source
    _bump_dataset_version()


def clear_cache() -> None:
    """Drop the cached dataframe without replacing it."""
    global _df_cache
    _df_cache = None


def filter_df(df: pd.DataFrame, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    """Filter by ISO date range [start, end)."""
    mask = pd.Series(True, index=df.index)
    if start:
        try:
            start_dt = pd.to_datetime(start, utc=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'start' date format. Use ISO like 2021-01-01.")
        mask &= df["ts"] >= start_dt
    if end:
        try:
            end_dt = pd.to_datetime(end, utc=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'end' date format. Use ISO like 2021-12-31.")
        mask &= df["ts"] < end_dt
    return df[mask]


def to_hours(ms: float) -> float:
    """Convert milliseconds -> hours (rounded)."""
    return round(float(ms) / 3_600_000, 3)
