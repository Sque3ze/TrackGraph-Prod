// src/App.js
import React, { useEffect, useLayoutEffect, useMemo, useState, useCallback, useRef } from "react";
import BubblesView from "./components/BubblesView";
import LeaderboardView from "./components/LeaderboardView";
import { formatHours, formatNumber, formatPercent } from "./utils/formatting";
import "./styles/index.css";
import "./index.css";

export default function App() {
  const headerRef = useRef(null);
  const toolbarRef = useRef(null);
  const [groupBy, setGroupBy] = useState("artist");
  const [filters, setFilters] = useState({ start: "", end: "" });
  const [details, setDetails] = useState(null);
  const [clearSignal, setClearSignal] = useState(0);
  const [view, setView] = useState("bubbles");
  const [dataSource, setDataSource] = useState(null);
  const detailsPanelRef = useRef(null);
  const [detailsScale, setDetailsScale] = useState(1);

  const viewFilters = useMemo(() => filters, [filters]);
  const dataReady = Boolean(dataSource);

  useEffect(() => {
    if (view !== "bubbles" && details) {
      setDetails(null);
      setClearSignal((s) => s + 1);
    }
  }, [view, details]);

  useLayoutEffect(() => {
    if (typeof window === "undefined") {
      return () => {};
    }

    const headerEl = headerRef.current;
    const toolbarEl = toolbarRef.current;
    if (!headerEl) {
      return () => {};
    }

    const rootStyle = document.documentElement.style;

    const updateHeaderMetrics = () => {
      const rect = headerEl.getBoundingClientRect();
      const headerHeight = Math.max(0, Math.round(rect.height));
      rootStyle.setProperty("--header-height", `${headerHeight}px`);

      const viewportHeight = window.innerHeight || document.documentElement?.clientHeight || 0;
      const gap = 20;
      rootStyle.setProperty("--bubble-gap", `${gap}px`);

      const availableHeight = Math.max(0, viewportHeight - headerHeight - gap * 2);
      const containerHeight = Math.max(320, Math.round(availableHeight || 0));
      rootStyle.setProperty("--bubble-container-height", `${containerHeight}px`);

      // Measure toolbar height to balance spacing with details panel
      if (toolbarEl) {
        const tRect = toolbarEl.getBoundingClientRect();
        const toolbarHeight = Math.max(0, Math.round(tRect.height));
        rootStyle.setProperty("--bubble-toolbar-height", `${toolbarHeight}px`);
      }
    };

    updateHeaderMetrics();

    const handleResize = () => updateHeaderMetrics();
    window.addEventListener("resize", handleResize);

    let resizeObserver;
    if (window.ResizeObserver) {
      resizeObserver = new ResizeObserver(updateHeaderMetrics);
      resizeObserver.observe(headerEl);
      if (toolbarEl) {
        resizeObserver.observe(toolbarEl);
      }
    }

    return () => {
      window.removeEventListener("resize", handleResize);
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
    };
  }, []);

  // Dynamically scale the details panel to avoid internal scrolling.
  useEffect(() => {
    if (typeof window === "undefined") return;

    const panelEl = detailsPanelRef.current;
    const headerEl = headerRef.current;
    const toolbarEl = toolbarRef.current;

    const recalcScale = () => {
      if (!panelEl || !details) {
        setDetailsScale(1);
        return;
      }
      const viewportHeight = window.innerHeight || document.documentElement?.clientHeight || 0;
      const headerHeight = headerEl ? Math.max(0, Math.round(headerEl.getBoundingClientRect().height)) : 0;
      const toolbarHeight = toolbarEl ? Math.max(0, Math.round(toolbarEl.getBoundingClientRect().height)) : 0;
      const gap = 20; // keep in sync with --bubble-gap

      const available = Math.max(0, viewportHeight - headerHeight - gap - toolbarHeight - gap);
      const natural = panelEl.scrollHeight || panelEl.getBoundingClientRect().height || 0;
      if (!available || !natural) {
        setDetailsScale(1);
        return;
      }
      const scale = Math.min(1, available / natural);
      setDetailsScale(scale > 0 ? scale : 1);
    };

    recalcScale();
    const onResize = () => recalcScale();
    window.addEventListener("resize", onResize);

    let ro;
    if (window.ResizeObserver && panelEl) {
      ro = new ResizeObserver(recalcScale);
      ro.observe(panelEl);
    }

    return () => {
      window.removeEventListener("resize", onResize);
      if (ro) ro.disconnect();
    };
  }, [details]);

  const handleEntityDetails = useCallback(
    (payload) => {
      setDetails(payload);
    },
    []
  );

  const handleClearSelection = useCallback(() => {
    setDetails(null);
    setClearSignal((s) => s + 1);
  }, []);

  return (
    <div className="app-shell">
      <header className="app-header" ref={headerRef}>
        <div className="app-header__inner">
          <div className="header-brand">
            <h1 className="app-title">
              Track Graph <span className="app-title-sub">– Your listening, visualized</span>
            </h1>
          </div>

          <div className="header-controls">
          <div className="view-switcher" role="group" aria-label="Change dashboard view">
            <button
              className={`nav-button ${view === "bubbles" ? "active" : ""}`}
              onClick={() => setView("bubbles")}
            >
              Bubbles
            </button>
            <button
              className={`nav-button ${view === "topstats" ? "active" : ""}`}
              onClick={() => setView("topstats")}
            >
              Top Stats
            </button>
          </div>

          <div className="date-range" aria-label="Filter by date range">
            <label className="date-field">
              <span>From</span>
              <input
                type="date"
                value={filters.start}
                onChange={(e) => setFilters((f) => ({ ...f, start: e.target.value }))}
                className="date-input"
                placeholder="Start date"
              />
            </label>
            <label className="date-field">
              <span>To</span>
              <input
                type="date"
                value={filters.end}
                onChange={(e) => setFilters((f) => ({ ...f, end: e.target.value }))}
                className="date-input"
                placeholder="End date"
              />
            </label>
          </div>
          </div>
        </div>
      </header>

      <main className="main-stage">
        {view === "bubbles" ? (
          <section className="bubble-stage">
            <div className={`bubble-layout ${details ? 'has-details' : ''}`}>
              <div className="bubble-toolbar" ref={toolbarRef}>
                <div className="bubble-group-toggle" role="radiogroup" aria-label="Group bubbles by">
                  <label className={`group-option ${groupBy === "artist" ? "active" : ""}`}>
                    <input
                      type="radio"
                      name="groupby"
                      value="artist"
                      checked={groupBy === "artist"}
                      onChange={() => setGroupBy("artist")}
                    />
                    <span>Artists</span>
                  </label>
                  <label className={`group-option ${groupBy === "album" ? "active" : ""}`}>
                    <input
                      type="radio"
                      name="groupby"
                      value="album"
                      checked={groupBy === "album"}
                      onChange={() => setGroupBy("album")}
                    />
                    <span>Albums</span>
                  </label>
                </div>
              </div>

              <div className="bubble-shell">

                <BubblesView
                  groupBy={groupBy}
                  filters={viewFilters}
                  onEntityDetails={handleEntityDetails}
                  clearSignal={clearSignal}
                  dataSource={dataSource}
                  onDataSourceChange={setDataSource}
                />
              </div>

              <aside
                ref={detailsPanelRef}
                className={`details-panel ${details ? "open" : ""} ${detailsScale < 0.999 ? "is-scaled" : ""}`}
                aria-hidden={!details}
                style={{ "--details-scale": String(detailsScale) }}
              >
                <div className="details-content">
                  <h2 className="details-title">Details</h2>
                  {details ? (
                    <div className="details-body animate-fade-in">
                      <div className="details-header">
                        <div
                          className={`details-art ${details.image_url ? "" : "details-art--fallback"}`}
                          style={details.image_url ? { backgroundImage: `url(${details.image_url})` } : undefined}
                        >
                          {!details.image_url && (
                            <span className="details-art-initial">
                              {(details.label || "?").slice(0, 1).toUpperCase()}
                            </span>
                          )}
                        </div>
                        <div className="details-heading">
                          <span className="details-tag">
                            {details.group_by === "album" ? "Album" : "Artist"}
                          </span>
                          <h3 className="details-name">{details.label}</h3>
                          {typeof details.value_pct === "number" && (
                            <span className="details-subtitle">
                              {formatPercent(details.value_pct)} of your listening time
                            </span>
                          )}
                        </div>
                      </div>

                      <div className="details-stats-grid">
                        <div className="details-stat-card">
                          <span className="details-stat-label">Listening Time</span>
                          <span className="details-stat-value">{formatHours(details.value_hours)}</span>
                        </div>
                        <div className="details-stat-card">
                          <span className="details-stat-label">Total Plays</span>
                          <span className="details-stat-value">{formatNumber(details.plays)}</span>
                        </div>
                      </div>

                      {details.top_tracks?.length ? (
                        <div className="details-tracks">
                          <div className="details-section-title">Top Tracks</div>
                          <ul className="track-list">
                            {details.top_tracks.map((track, index) => {
                              const trackName = track.name || track.master_metadata_track_name || "Unknown Track";
                              const trackHours = track.hours ?? track.value_hours ?? (track.ms ? track.ms / 3600000 : 0);
                              const trackPlays = track.plays ?? track.track_plays ?? 0;
                              const shareRaw = details.value_hours > 0 ? trackHours / details.value_hours : 0;
                              const share = Math.max(0, Math.min(shareRaw || 0, 1));
                              const progressWidth = share > 0 ? share * 100 : 4;

                              return (
                                <li key={`${trackName}-${index}`} className="track-item">
                                  <div className="track-rank">#{index + 1}</div>
                                  <div className="track-info">
                                    <div className="track-name-row">
                                      <span className="track-name">{trackName}</span>
                                      <span className="track-duration">{formatHours(trackHours)}</span>
                                    </div>
                                    <div className="track-meta">
                                      <span>{formatNumber(trackPlays)} plays</span>
                                      <span>{formatPercent(share)} of time</span>
                                    </div>
                                    <div className="track-progress">
                                      <div
                                        className="track-progress-bar"
                                        style={{ width: `${Math.min(progressWidth, 100)}%` }}
                                      />
                                    </div>
                                  </div>
                                </li>
                              );
                            })}
                          </ul>
                        </div>
                      ) : null}

                      <div className="details-footer">
                        <button className="clear-selection-btn" onClick={handleClearSelection}>
                          Clear selection
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="details-placeholder">
                      <div className="placeholder-icon">O</div>
                      <p>Click a bubble to explore</p>
                      <p className="placeholder-subtitle">Discover your listening patterns</p>
                    </div>
                  )}
                </div>
              </aside>
            </div>
          </section>
        ) : (
          <section className="topstats-stage">
            <div className="topstats-shell">
              <LeaderboardView filters={viewFilters} dataReady={dataReady} />
            </div>
          </section>
        )}
      </main>
    </div>
  );
}
