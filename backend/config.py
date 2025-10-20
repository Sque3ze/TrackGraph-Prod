"""Configuration helpers and constants for the TrackGraph backend."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

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
    """Parse a boolean environment variable with sane defaults."""
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


# Spotify API credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
DEMO_MODE = _env_bool("DEMO_MODE", False)


# Dataset locations
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
