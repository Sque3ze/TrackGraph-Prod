// src/hooks/useSpotifyData.js
import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { apiUrl, fetchJSON } from "../services/api";

const cache = new Map(); // key -> { data, ts }
const CACHE_TTL_MS = 60_000;

export function clearSpotifyDataCache() {
  cache.clear();
}

export function useSpotifyData(resource, params = {}, options = {}) {
  const { enabled = true } = options;
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(() => Boolean(enabled));
  const abortRef = useRef(null);
  const paramsRef = useRef(params || {});

  const paramsKey = useMemo(() => {
    try {
      return JSON.stringify(params || {});
    } catch (err) {
      return "__invalid_params__";
    }
  }, [params]);

  useEffect(() => {
    paramsRef.current = params || {};
  }, [paramsKey, params]);

  const load = useCallback(
    async (overrideParams) => {
      const base = paramsRef.current || {};
      const p = { ...base, ...(overrideParams || {}) };
      const requestKey = JSON.stringify({ resource, params: p, cacheSeed: paramsKey });

      // cache
      const hit = cache.get(requestKey);
      if (hit && Date.now() - hit.ts < CACHE_TTL_MS) {
        setData(hit.data);
        setError(null);
        setLoading(false);
        return hit.data;
      }

      // abort any in-flight
      if (abortRef.current) abortRef.current.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;

      setLoading(true);
      setError(null);

      try {
        let url;
        if (resource === "bubbles") {
          // GET /api/bubbles?group_by&start&end&limit
          url = apiUrl("/api/bubbles", p); // mirrors the current app's call
        } else if (resource === "summary") {
          url = apiUrl("/api/summary", p);
        } else if (resource === "historical") {
          url = apiUrl("/api/historical_data", p);
        } else {
          throw new Error(`Unknown resource: ${resource}`);
        }

        const json = await fetchJSON(url, { signal: ctrl.signal });
        if (json && json.timings) {
          // Surface server timings in the console for diagnosis
          try {
            // eslint-disable-next-line no-console
            console.log(`[api] ${resource} timings`, json.timings);
          } catch (_) {}
        }
        // Normalize field names used by BubbleChart.
        const normalized = normalizeResponse(resource, json);
        cache.set(requestKey, { data: normalized, ts: Date.now() });

        setData(normalized);
        setLoading(false);
        return normalized;
      } catch (e) {
        if (e.name === "AbortError") return;
        setError(e);
        setLoading(false);
      }
    },
    [resource, paramsKey]
  );

  useEffect(() => {
    if (!enabled) {
      if (abortRef.current) {
        abortRef.current.abort();
        abortRef.current = null;
      }
      setLoading(false);
      setError(null);
      setData(null);
      return undefined;
    }

    load();
    return () => abortRef.current?.abort();
  }, [load, enabled]);

  const refetch = useCallback((nextParams) => load(nextParams), [load]);

  // Entity details for right panel (artist/album mini profile)
  const fetchEntityDetails = useCallback(async (groupBy, id, filters = {}) => {
    // This can route to a lightweight server helper later.
    // For now, return the matching bubble + small selection of top tracks.
    const d = data?.items?.find((x) => x.id === id);
    if (!d) return null;
    return {
      id: d.id,
      group_by: groupBy,
      label: d.label,
      plays: d.plays,
      value_hours: d.value_hours,
      value_pct: d.value_pct,
      distinct_tracks: d.distinct_tracks,
      top_tracks: d.top_tracks?.slice(0, 10) || [],
      image_url: d.image_url,
      artist: d.artist,
      filters,
    };
  }, [data]);

  return { data, loading, error, refetch, fetchEntityDetails };
}

function normalizeResponse(resource, json) {
  if (resource !== "bubbles") return json;

  const items = (json.items || []).map((d) => ({
    // Ensure fields BubbleChart expects based on the current chart.
    id: d.id,
    label: d.label,
    value_ms: d.value_ms ?? d.ms_total ?? 0,
    value_hours: d.value_hours ?? d.ms_total / 3_600_000 ?? 0,
    value_pct: d.value_pct ?? d.pct ?? 0,
    plays: d.plays ?? 0,
    distinct_tracks: d.distinct_tracks ?? 0,
    top_tracks: d.top_tracks || [],
    // Representative IDs for image hydration (albums/artist); matches the server aggregations.
    ids: d.ids || d.id_list || [],
    artist: d.artist,
    image_url: d.image_url || null,
    representative_track_id: d.representative_track_id || null,
  }));

  return {
    ...json,
    items,
    group_by: json.group_by || "artist",
    total_hours: json.total_hours ?? (json.total_ms ? json.total_ms / 3_600_000 : undefined),
    total_plays: json.total_plays,
  };
}
