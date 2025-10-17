// components/LeaderboardView.jsx
import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { LeaderboardChart } from "../visualizations/LeaderboardChart";
import { useSpotifyData } from "../hooks/useSpotifyData";
import { apiUrl, fetchJSON } from "../services/api";

export default function LeaderboardView({ filters, dataReady = true }) {
  const containerRef = useRef();
  const chartRef = useRef();
  const [rows, setRows] = useState({ artists: [], albums: [], tracks: [] });
  const [selection, setSelection] = useState(null);

  useEffect(() => {
    if (!dataReady) {
      setRows({ artists: [], albums: [], tracks: [] });
      setSelection(null);
    }
  }, [dataReady]);

  const historicalParams = useMemo(
    () => ({ ...(filters || {}), limit: 200 }),
    [filters]
  );
  const { data, loading, error } = useSpotifyData("historical", historicalParams, { enabled: dataReady });

  // Build simple arrays from historical payload
  const prepared = useMemo(() => {
    if (!data) return null;
    const aa_to_id = data.artist_album_to_id || {};
    const albumToArtist = new Map(
      Object.keys(aa_to_id).map((k) => {
        const [artist, album] = k.split("::");
        return [album, artist];
      })
    );

    const toHours = (ms) => Math.round((ms / 3_600_000) * 100) / 100;

    const artists = (data.artists || [])
      .map((r) => ({
        id: r.master_metadata_album_artist_name,
        name: r.master_metadata_album_artist_name,
        streams: r.plays,
        hours: toHours(r.ms_played),
      }))
      .sort((a, b) => {
        const hourDiff = b.hours - a.hours;
        return Math.abs(hourDiff) > 0.0001 ? hourDiff : (b.streams - a.streams);
      });

    const albums = (data.albums || [])
      .map((r) => ({
        id: r.master_metadata_album_album_name,
        name: r.master_metadata_album_album_name,
        artist: albumToArtist.get(r.master_metadata_album_album_name) || undefined,
        streams: r.plays,
        hours: toHours(r.ms_played),
      }))
      .sort((a, b) => {
        const hourDiff = b.hours - a.hours;
        return Math.abs(hourDiff) > 0.0001 ? hourDiff : (b.streams - a.streams);
      });

    const tracks = (data.tracks || [])
      .map((r) => ({
        id: r.master_metadata_track_name,
        name: r.master_metadata_track_name,
        track_id: r.track_id,
        streams: r.plays,
        hours: toHours(r.ms_played),
      }))
      .sort((a, b) => {
        const hourDiff = b.hours - a.hours;
        return Math.abs(hourDiff) > 0.0001 ? hourDiff : (b.streams - a.streams);
      });

    const repTrackByArtist = new Map();
    Object.entries(aa_to_id).forEach(([key, trackId]) => {
      const [artist] = key.split("::");
      if (artist && trackId && !repTrackByArtist.has(artist)) {
        repTrackByArtist.set(artist, trackId);
      }
    });

    return { artists, albums, tracks, aa_to_id, repTrackByArtist };
  }, [data]);

  // Hydrate images for the top-5 of each row (rest render as names; optional to extend)
  useEffect(() => {
    if (!prepared) return;

    const MAX_ITEMS = 103;
    const truncateClone = (arr) => arr.slice(0, MAX_ITEMS).map((d) => ({ ...d }));

    const artists = truncateClone(prepared.artists);
    const albums = truncateClone(prepared.albums);
    const tracks = truncateClone(prepared.tracks);

    tracks.forEach((track) => {
      if (track.track_id) {
        track.id = track.track_id;
      }
    });

    (async () => {
      const trackIdsNeeded = new Set();
      const albumTrackById = new Map();
      const artistRepTrack = new Map();

      albums.forEach((album) => {
        if (!album.artist) return;
        const repId = prepared.aa_to_id[`${album.artist}::${album.name}`];
        if (repId) {
          albumTrackById.set(album.id, repId);
          trackIdsNeeded.add(repId);
        }
      });

      tracks.forEach((track) => {
        if (track.track_id) {
          trackIdsNeeded.add(track.track_id);
        }
        track.spotify_url = track.track_id
          ? `https://open.spotify.com/track/${track.track_id}`
          : null;
      });

      artists.forEach((artist) => {
        const rep = prepared.repTrackByArtist?.get?.(artist.name);
        if (rep) {
          artistRepTrack.set(artist.name, rep);
          trackIdsNeeded.add(rep);
        }
      });

      let trackMetaById = new Map();
      if (trackIdsNeeded.size) {
        try {
          const tracksRes = await fetchJSON(
            apiUrl("/api/tracks-batch", { ids: Array.from(trackIdsNeeded).join(",") })
          );
          const trackItems = Array.isArray(tracksRes)
            ? tracksRes
            : tracksRes.tracks || tracksRes.items || [];
          trackMetaById = new Map(
            trackItems
              .filter((t) => t && (t.id || t.track?.id))
              .map((t) => [t.id || t.track?.id, t.track || t])
          );
        } catch (err) {
        }
      }

      albums.forEach((album) => {
        const repId = albumTrackById.get(album.id);
        if (!repId) return;
        const meta = trackMetaById.get(repId);
        const img = meta?.album?.images?.[0]?.url || null;
        if (img) album.image = img;
        const albumId = meta?.album?.id;
        if (albumId) {
          album.album_id = albumId;
          album.spotify_url = meta?.album?.external_urls?.spotify || `https://open.spotify.com/album/${albumId}`;
        }
      });

      tracks.forEach((track) => {
        const meta = trackMetaById.get(track.track_id);
        const img = meta?.album?.images?.[0]?.url || null;
        if (img) track.image = img;
        if (meta) {
          track.spotify_url = meta?.external_urls?.spotify || track.spotify_url;
          track.duration_ms = meta?.duration_ms;
          track.album_name = meta?.album?.name;
          track.album_id = meta?.album?.id;
          track.primary_artist = meta?.artists?.[0]?.name;
        }
      });

      const artistIdForName = new Map();
      artists.forEach((artist) => {
        const repId = artistRepTrack.get(artist.name);
        if (!repId) return;
        const meta = trackMetaById.get(repId);
        if (!meta) return;
        const match = (meta.artists || []).find(
          (entry) => entry?.name?.toLowerCase() === artist.name.toLowerCase()
        ) || (meta.artists || [])[0];
        if (match?.id) {
          artistIdForName.set(artist.name, match.id);
        }
      });

      if (artistIdForName.size) {
        try {
          const artistRes = await fetchJSON(
            apiUrl("/api/artists-batch", { ids: Array.from(new Set(artistIdForName.values())).join(",") })
          );
          const artistItems = Array.isArray(artistRes)
            ? artistRes
            : artistRes.artists || [];
          const artistMeta = new Map(
            artistItems
              .filter((a) => a && a.id)
              .map((a) => [a.id, a])
          );
          artists.forEach((artist) => {
            const aid = artistIdForName.get(artist.name);
            if (!aid) return;
            const meta = artistMeta.get(aid);
            const img = meta?.images?.[0]?.url || null;
            if (img) artist.image = img;
            artist.spotify_id = meta?.id;
            artist.genres = meta?.genres || [];
            artist.spotify_url = meta?.external_urls?.spotify || `https://open.spotify.com/artist/${meta?.id}`;
          });
        } catch (err) {
          // ignore failures; text rows still render
        }
      }

      // Fallback URLs for albums without resolved IDs (link to representative track)
      albums.forEach((album) => {
        if (!album.spotify_url) {
          const repId = albumTrackById.get(album.id);
          if (repId) {
            album.spotify_url = `https://open.spotify.com/track/${repId}`;
          }
        }
      });

      artists.forEach((artist) => {
        if (!artist.spotify_url) {
          const rep = artistRepTrack.get(artist.name);
          if (rep) {
            artist.spotify_url = `https://open.spotify.com/track/${rep}`;
          }
        }
      });

      setRows({ artists, albums, tracks });
    })();
  }, [prepared]);

  const handleItemSelect = useCallback((kind, item, rect) => {
    if (!item) return;
    setSelection({ kind, item, rect });
  }, []);

  useEffect(() => {
    if (!dataReady) {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
      return undefined;
    }

    if (!containerRef.current) {
      return undefined;
    }

    if (chartRef.current) {
      chartRef.current.destroy();
      chartRef.current = null;
    }

    chartRef.current = new LeaderboardChart(containerRef, {
      onItemSelect: handleItemSelect,
    });
    chartRef.current.render(rows);

    return () => {
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, [rows, handleItemSelect, dataReady]);

  useEffect(() => {
    if (!selection) return;
    const onKey = (e) => {
      if (e.key === "Escape") {
        setSelection(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selection]);

  const closeSelection = useCallback(() => setSelection(null), []);

  if (!dataReady) {
    return (
      <div className="leaderboard-placeholder">
        <div className="leaderboard-placeholder__panel">
          <h2>Upload data to explore stats</h2>
          <p>Choose a CSV or the sample dataset on the bubbles view to unlock the leaderboard.</p>
        </div>
      </div>
    );
  }

  if (loading) return <div className="bubbles-container loading"><div className="loading-spinner"/></div>;
  if (error) return <div className="error-message">Failed to load leaderboard: {error.message}</div>;

  return (
      <>
        <div ref={containerRef} className="leaderboard-wrapper" />
        {selection ? (
          <DetailOverlay selection={selection} onClose={closeSelection} />
        ) : null}
      </>
  );
}

function DetailOverlay({ selection, onClose }) {
  const { kind, item, rect } = selection;
  const [expanded, setExpanded] = useState(false);
  const [hasMeasured, setHasMeasured] = useState(false);
  const cardRef = useRef(null);

  const finalWidth = Math.max(280, Math.min(460, window.innerWidth - 32));

  const [style, setStyle] = useState({
    left: rect.left,
    top: rect.top,
    width: rect.width,
    height: rect.height,
  });

  useEffect(() => {
    setExpanded(false);
    setHasMeasured(false);
    setStyle({
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height,
    });

    const frame = requestAnimationFrame(() => {
      setExpanded(true);
      setStyle((prev) => ({
        ...prev,
        left: window.innerWidth / 2 - finalWidth / 2,
        width: finalWidth,
      }));
    });

    return () => cancelAnimationFrame(frame);
  }, [rect.left, rect.top, rect.width, rect.height, finalWidth]);

  useLayoutEffect(() => {
    if (!expanded || hasMeasured || !cardRef.current) return;
    const el = cardRef.current;
    const naturalHeight = el.scrollHeight;
    const viewportAllowance = Math.max(220, window.innerHeight - 80);

    const art = el.querySelector(".lb-overlay-art img");
    const artHeight = art ? art.getBoundingClientRect().height : 130;
    const bodyVerticalPadding = 56; // 28px top + 28px bottom padding in CSS
    const minExpandedHeight = artHeight + bodyVerticalPadding;

    const targetHeight = Math.min(
      Math.max(naturalHeight, minExpandedHeight),
      viewportAllowance
    );
    const top = Math.max(40, (window.innerHeight - targetHeight) / 2);

    requestAnimationFrame(() => {
      setStyle((prev) => ({
        ...prev,
        top,
        height: targetHeight,
      }));
      setHasMeasured(true);
    });
  }, [expanded, hasMeasured]);

  const spotifyUrl = item.spotify_url || fallbackSpotifyUrl(kind, item);
  const image = item.image || null;
  const placeholder = "https://via.placeholder.com/140?text=♫";
  const plays = typeof item.streams === "number" ? item.streams.toLocaleString() : "-";
  const hours = typeof item.hours === "number" ? item.hours.toFixed(2) : "0.00";
  const duration = kind === "track" && item.duration_ms
    ? formatDuration(item.duration_ms)
    : null;

  return (
    <div className="lb-overlay" onClick={onClose}>
      <div
        ref={cardRef}
        className={`lb-overlay-card ${expanded ? "expanded" : ""}`}
        style={style}
        onClick={(e) => e.stopPropagation()}
      >
        <button className="lb-overlay-close" onClick={onClose} aria-label="Close">
          ×
        </button>
        <div className="lb-overlay-body">
          <div className="lb-overlay-art">
            <a href={spotifyUrl} target="_blank" rel="noopener noreferrer">
              {image ? (
                <img src={image} alt="" />
              ) : (
                <img src={placeholder} alt="" />
              )}
            </a>
          </div>
          <div className="lb-overlay-content">
            <h3 className="lb-overlay-title">{item.name}</h3>
            <p className="lb-overlay-meta">{kindLabel(kind)} • {plays} plays</p>
            <p className="lb-overlay-meta">{hours} hrs listened</p>
            {kind === "track" && item.album_name ? (
              <p className="lb-overlay-note">Album: {item.album_name}{item.primary_artist ? ` • ${item.primary_artist}` : ""}</p>
            ) : null}
            {kind === "album" && item.artist ? (
              <p className="lb-overlay-note">Artist: {item.artist}</p>
            ) : null}
            {kind === "artist" && item.genres?.length ? (
              <p className="lb-overlay-note">Genres: {item.genres.slice(0, 4).join(", ")}</p>
            ) : null}
            {duration ? (
              <p className="lb-overlay-meta">Duration: {duration}</p>
            ) : null}
            <a className="lb-overlay-link" href={spotifyUrl} target="_blank" rel="noopener noreferrer">
              Open in Spotify →
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

function fallbackSpotifyUrl(kind, item) {
  if (kind === "track" && item.track_id) {
    return `https://open.spotify.com/track/${item.track_id}`;
  }
  if (kind === "album") {
    if (item.album_id) {
      return `https://open.spotify.com/album/${item.album_id}`;
    }
    if (item.spotify_url) return item.spotify_url;
  }
  if (kind === "artist" && item.spotify_id) {
    return `https://open.spotify.com/artist/${item.spotify_id}`;
  }
  return "https://open.spotify.com";
}

function kindLabel(kind) {
  if (kind === "artist") return "Artist";
  if (kind === "album") return "Album";
  return "Track";
}

function formatDuration(ms) {
  const totalSeconds = Math.round(ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}
