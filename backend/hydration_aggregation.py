"""Aggregation utilities for TrackGraph analytics responses."""

from __future__ import annotations

import time
from typing import Any, Dict, Literal, Optional

import pandas as pd
from fastapi import HTTPException

try:
    from backend import history
except ModuleNotFoundError:
    import history

# Cache derived dictionaries per dataset + filter key to avoid recomputation.
_build_dicts_cache_map: Dict[tuple, tuple[dict, dict, dict]] = {}


def _clear_dict_cache(_version: int) -> None:
    _build_dicts_cache_map.clear()


history.register_on_change(_clear_dict_cache)


def _unique_non_null(values) -> list[str]:
    """Return unique, truthy values preserving order."""
    unique = []
    for val in pd.unique(values):
        if pd.notna(val) and val:
            unique.append(str(val))
    return unique


def build_dicts(df: pd.DataFrame, filter_key: Optional[tuple] = None):
    """Build and cache mappings used across endpoints."""
    dataset_version = history.get_dataset_version()
    key = (dataset_version, filter_key)
    hit = _build_dicts_cache_map.get(key)
    if hit is not None:
        return hit

    historical_albums = (
        df.groupby(["master_metadata_album_artist_name", "master_metadata_album_album_name"], dropna=False)
        .agg(
            ms_played=("ms_played", "sum"),
            plays=("ts", "count"),
            distinct_tracks=("master_metadata_track_name", "nunique"),
            ids=("track_id", "first"),
        )
        .reset_index()
    )
    aa_to_id_tuple = (
        historical_albums
        .set_index(["master_metadata_album_artist_name", "master_metadata_album_album_name"])["ids"]
        .to_dict()
    )

    ta_to_album_tuple = (
        df.set_index(["master_metadata_track_name", "master_metadata_album_artist_name"])["master_metadata_album_album_name"]
        .to_dict()
    )

    artist_album_to_id = {
        f"{artist}::{album}": track_id
        for (artist, album), track_id in aa_to_id_tuple.items()
    }
    id_to_artist_album = {
        track_id: f"{artist}::{album}"
        for (artist, album), track_id in aa_to_id_tuple.items()
    }
    track_artist_to_album = {
        f"{track}::{artist}": album
        for (track, artist), album in ta_to_album_tuple.items()
    }

    value = (artist_album_to_id, id_to_artist_album, track_artist_to_album)
    _build_dicts_cache_map[key] = value
    return value


def _hydrate_bubble_images(
    items: list[dict],
    group_by: Literal["artist", "album"],
    artist_album_to_id: dict,
    track_artist_to_album: dict,
    image_cache: Any,
    batch_tracks_fn,
    batch_artists_fn,
    stats: Optional[dict] = None,
) -> None:
    """Attach best-effort image metadata to bubble items."""
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

    fetched_any = False
    if missing_track_ids:
        try:
            track_payloads = batch_tracks_fn(tuple(missing_track_ids))
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
                artist_payloads = batch_artists_fn(tuple(missing_artist_ids))
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


def aggregates(
    df: pd.DataFrame,
    group_by: Literal["artist", "album"],
    *,
    image_cache: Any,
    batch_tracks_fn,
    batch_artists_fn,
    filter_key: Optional[tuple] = None,
) -> dict:
    """Compute bubble items for artist/album groups and hydrate images."""
    _t0 = time.perf_counter()
    if group_by == "artist":
        key_col = "master_metadata_album_artist_name"
    else:
        key_col = "master_metadata_album_album_name"

    _t_dicts_start = time.perf_counter()
    aa_id, id_aa, ta_a = build_dicts(df, filter_key)
    _t_dicts_end = time.perf_counter()

    artist_playtime = df.groupby("master_metadata_album_artist_name")["ms_played"].sum()
    artists_over_1hr = artist_playtime[artist_playtime > 3_600_000].index

    album_playtime = df.groupby("master_metadata_album_album_name")["ms_played"].sum()
    albums_over_1hr = album_playtime[album_playtime > 3_600_000].index

    total_ms = int(df["ms_played"].sum())
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

    tt = (
        df.groupby([key_col, "master_metadata_track_name"], dropna=False)
        .agg(track_ms=("ms_played", "sum"), track_plays=("ts", "count"))
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
                "hours": history.to_hours(r["track_ms"]),
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
            "value_hours": history.to_hours(ms_total),
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
    _hydrate_bubble_images(
        items,
        group_by,
        artist_album_to_id=aa_id,
        track_artist_to_album=ta_a,
        image_cache=image_cache,
        batch_tracks_fn=batch_tracks_fn,
        batch_artists_fn=batch_artists_fn,
        stats=hydrate_stats,
    )
    _t_hydrate_end = time.perf_counter()

    _t1 = time.perf_counter()
    return {
        "group_by": group_by,
        "total_ms": total_ms,
        "total_hours": history.to_hours(total_ms),
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
        },
    }


def historical_data(df: pd.DataFrame, limit: Optional[int] = None, filter_key: Optional[tuple] = None) -> dict:
    """Return top artists/albums/tracks with simple counts."""
    aa_id, id_aa, ta_a = build_dicts(df, filter_key)

    historical_artists = (
        df.groupby(["master_metadata_album_artist_name"], dropna=False)
        .agg(
            ms_played=("ms_played", "sum"),
            plays=("ts", "count"),
            distinct_tracks=("master_metadata_track_name", "nunique"),
        )
        .reset_index()
    ).sort_values("plays", ascending=False)

    historical_albums = (
        df.groupby(["master_metadata_album_album_name"], dropna=False)
        .agg(
            ms_played=("ms_played", "sum"),
            plays=("ts", "count"),
            distinct_tracks=("master_metadata_track_name", "nunique"),
        )
        .reset_index()
    ).sort_values("plays", ascending=False)

    historical_tracks = (
        df.groupby(["master_metadata_track_name", "spotify_track_uri"], dropna=False)
        .agg(ms_played=("ms_played", "sum"), plays=("ts", "count"))
        .reset_index()
    ).sort_values("plays", ascending=False)

    historical_tracks["spotify_track_uri"] = historical_tracks["spotify_track_uri"].str.replace(
        "spotify:track:", "",
        regex=False,
    )
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
        "track_artist_to_album": ta_a,
    }
