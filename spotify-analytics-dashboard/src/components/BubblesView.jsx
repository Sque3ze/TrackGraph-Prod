// components/BubblesView.jsx
import React, { useRef, useEffect, useState, useCallback, useMemo } from 'react';
import { BubbleChart } from '../visualizations/BubbleChart';
import { useSpotifyData, clearSpotifyDataCache } from '../hooks/useSpotifyData';
import { formatHours, formatPercent, formatNumber } from '../utils/formatting';
import Tooltip from './Tooltip';
import { apiUrl, fetchJSON } from '../services/api';

const BubblesView = ({
  groupBy,
  filters,
  onSummaryUpdate,
  onEntityDetails = () => {},
  clearSignal,
  dataSource,
  onDataSourceChange = () => {},
}) => {
  const svgRef = useRef();
  const containerRef = useRef();
  const chartRef = useRef();
  const tooltipStateRef = useRef({ bubbleId: null, content: '', x: 0, y: 0, rafId: null });
  const [tooltip, setTooltip] = useState({ show: false, x: 0, y: 0, content: '' });
  const [containerDimensions, setContainerDimensions] = useState({ width: 600, height: 500 });
  const [isDragActive, setIsDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState('');
  const fileInputRef = useRef(null);
  const defaultReadyRef = useRef(false);
  const prewarmedKeysRef = useRef(new Set());

  const dataReady = Boolean(dataSource);
  const resetSelection = useCallback(() => {
    if (chartRef.current) {
      chartRef.current.clearSelection();
    }
    onEntityDetails(null);
  }, [onEntityDetails]);

  const handleDataSourceReady = useCallback((source) => {
    clearSpotifyDataCache();
    resetSelection();
    onDataSourceChange(source);
  }, [onDataSourceChange, resetSelection]);

  const uploadHistoryFromFile = useCallback(async (file) => {
    if (!file) return;

    setUploading(true);
    setUploadError('');
    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch(apiUrl('/api/upload_history'), {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const text = await response.text();
        let message = text || 'Upload failed.';
        try {
          const parsed = JSON.parse(text);
          message = parsed?.detail || parsed?.error || message;
        } catch (err) {
          // ignore JSON parse errors
        }
        throw new Error(message);
      }

      await response.json();
      handleDataSourceReady('uploaded');
    } catch (err) {
      const message = err?.message || 'Upload failed. Please try again.';
      setUploadError(message);
    } finally {
      setIsDragActive(false);
      setUploading(false);
    }
  }, [handleDataSourceReady]);

  const handleFileList = useCallback((fileList) => {
    if (!fileList || !fileList.length || uploading) return;
    const [file] = fileList;
    if (file) {
      const lname = (file.name || '').toLowerCase();
      if (!(lname.endsWith('.csv') || lname.endsWith('.json'))) {
        setUploadError('Please upload a CSV or JSON file exported from Spotify.');
        setIsDragActive(false);
        return;
      }
      uploadHistoryFromFile(file);
    }
  }, [uploadHistoryFromFile, uploading]);

  const handleFileInputChange = useCallback((event) => {
    handleFileList(event.target.files);
    event.target.value = '';
  }, [handleFileList]);

  const handleBrowseClick = useCallback(() => {
    if (uploading) return;
    fileInputRef.current?.click();
  }, [uploading]);

  const handleUseDefault = useCallback(async () => {
    if (uploading) return;
    setUploadError('');
    // If default was already prewarmed, avoid a second POST; just flip the gate
    if (defaultReadyRef.current) {
      handleDataSourceReady('default');
      return;
    }
    setUploading(true);
    try {
      const response = await fetch(apiUrl('/api/use_default_history'), { method: 'POST' });
      if (!response.ok) {
        const text = await response.text();
        let message = text || 'Failed to enable sample data.';
        try { const parsed = JSON.parse(text); message = parsed?.detail || parsed?.error || message; } catch {}
        throw new Error(message);
      }
      await response.json();
      defaultReadyRef.current = true;
      handleDataSourceReady('default');
    } catch (err) {
      const message = err?.message || 'Failed to enable sample data.';
      setUploadError(message);
    } finally {
      setIsDragActive(false);
      setUploading(false);
    }
  }, [handleDataSourceReady, uploading]);

  const handleDragOver = useCallback((event) => {
    event.preventDefault();
    if (uploading) return;
    event.dataTransfer.dropEffect = 'copy';
    setIsDragActive(true);
  }, [uploading]);

  const handleDragLeave = useCallback((event) => {
    event.preventDefault();
    if (event.relatedTarget && event.currentTarget.contains(event.relatedTarget)) {
      return;
    }
    setIsDragActive(false);
  }, []);

  const handleDrop = useCallback((event) => {
    event.preventDefault();
    if (uploading) return;
    setIsDragActive(false);
    const files = event.dataTransfer?.files;
    if (files && files.length) {
      handleFileList(files);
    }
  }, [handleFileList, uploading]);

  const bubbleParams = useMemo(() => ({
    ...(filters || {}),
    group_by: groupBy,
  }), [filters, groupBy]);

  const {
    data: bubblesData,
    loading,
    error,
    fetchEntityDetails,
  } = useSpotifyData('bubbles', bubbleParams, { enabled: dataReady });

  // Prewarm default dataset in the background before user selects an option
  useEffect(() => {
    if (dataReady) return; // only prewarm when gated
    const prewarmKey = `default::${JSON.stringify(bubbleParams)}`;
    if (prewarmedKeysRef.current.has(prewarmKey)) return;

    let aborted = false;
    prewarmedKeysRef.current.add(prewarmKey);

    (async () => {
      try {
        // Ensure the default dataset is loaded server-side.
        const res = await fetch(apiUrl('/api/use_default_history'), { method: 'POST' });
        if (!res.ok) throw new Error('prewarm: use_default_history failed');

        // Prewarm artist bubbles for the current filters.
        const artistUrl = apiUrl('/api/bubbles', { ...bubbleParams, group_by: 'artist' });
        console.time('[prewarm] bubbles:artist');
        await fetchJSON(artistUrl);
        console.timeEnd('[prewarm] bubbles:artist');
        if (aborted) return;
        // Mark default ready once the first bubbles payload is cached
        defaultReadyRef.current = true;

        // Defer album/historical prefetch to the post-render effect to reduce contention.
      } catch (err) {
        // Silent failure - prewarm is best-effort.
        // console.debug('Prewarm failed:', err);
      }
    })();

    return () => { aborted = true; };
  }, [dataReady, bubbleParams, filters]);

  // Measure container size and pass dimensions to the chart
  useEffect(() => {
    if (typeof window === 'undefined') {
      return () => {};
    }

    const el = containerRef.current;
    if (!el) {
      return () => {};
    }

    let frameId = null;
    const measure = () => {
      frameId = null;
      const width = Math.floor(el.clientWidth || el.getBoundingClientRect().width || 0);
      const height = Math.floor(el.clientHeight || el.getBoundingClientRect().height || 0);

      if (!width || !height) {
        return;
      }

      setContainerDimensions((prev) => (
        prev.width === width && prev.height === height
          ? prev
          : { width, height }
      ));
    };

    const scheduleMeasure = () => {
      if (frameId) {
        cancelAnimationFrame(frameId);
      }
      frameId = requestAnimationFrame(measure);
    };

    scheduleMeasure();
    window.addEventListener('resize', scheduleMeasure);

    let resizeObserver;
    if (window.ResizeObserver) {
      resizeObserver = new ResizeObserver(scheduleMeasure);
      resizeObserver.observe(el);
    }

    return () => {
      window.removeEventListener('resize', scheduleMeasure);
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
      if (frameId) {
        cancelAnimationFrame(frameId);
      }
    };
  }, []);

  // Update parent summary
  useEffect(() => {
    if (bubblesData && onSummaryUpdate) {
      onSummaryUpdate({
        total_hours: bubblesData.total_hours,
        total_plays: bubblesData.total_plays,
        items_count: bubblesData.items?.length || 0,
        group_by: bubblesData.group_by,
      });
    }
  }, [bubblesData, onSummaryUpdate]);

  const handleBubbleClick = useCallback(async (bubble) => {
    chartRef.current.selectBubble(bubble.id);
    try {
      const details = await fetchEntityDetails(groupBy, bubble.id, filters);
      if (details) onEntityDetails(details);
    } catch (err) {
      console.error('Failed to fetch entity details:', err);
    }
  }, [groupBy, filters, fetchEntityDetails, onEntityDetails]);

  const handleBubbleHover = useCallback((event, bubble) => {
    const state = tooltipStateRef.current;
    if (state.bubbleId !== bubble.id) {
      const topTracks = bubble.top_tracks
        ? bubble.top_tracks.slice(0, 3).map(t =>
          `• ${t.name || t.master_metadata_track_name} (${formatHours(t.hours || t.value_hours)})`
        ).join('<br>')
        : '';

      state.content = `
        <div class="font-bold text-lg mb-2">${bubble.label}</div>
        <div class="text-green-400 text-sm mb-2">
          ${formatHours(bubble.value_hours)} • ${formatPercent(bubble.value_pct)}
        </div>
        <div class="text-xs mb-2">
          ${formatNumber(bubble.plays)} plays • ${bubble.distinct_tracks} tracks
        </div>
        ${topTracks ? `<div class="text-xs border-t border-gray-600 pt-2">${topTracks}</div>` : ''}
      `;
      state.bubbleId = bubble.id;
    }

    state.x = event.clientX + 12;
    state.y = event.clientY + 12;

    if (!state.rafId) {
      state.rafId = requestAnimationFrame(() => {
        state.rafId = null;
        setTooltip({
          show: true,
          x: state.x,
          y: state.y,
          content: state.content,
        });
      });
    }
  }, []);

  const handleBubbleLeave = useCallback(() => {
    const state = tooltipStateRef.current;
    if (state.rafId) {
      cancelAnimationFrame(state.rafId);
      state.rafId = null;
    }
    state.bubbleId = null;
    state.content = '';
    setTooltip({ show: false, x: 0, y: 0, content: '' });
  }, []);

  // Render chart
  useEffect(() => {
    if (loading || error) return;
    if (!svgRef.current || !bubblesData) return;
    if (!containerDimensions.width || !containerDimensions.height) return;

    let chart = chartRef.current;

    if (!chart) {
      chart = new BubbleChart(svgRef, {
        onBubbleClick: handleBubbleClick,
        onBubbleHover: handleBubbleHover,
        onBubbleLeave: handleBubbleLeave,
        physicsEnabled: true,
      });
      chartRef.current = chart;
    } else {
      chart.onBubbleClick = handleBubbleClick;
      chart.onBubbleHover = handleBubbleHover;
      chart.onBubbleLeave = handleBubbleLeave;
    }

    chart.render(bubblesData, containerDimensions);

    if (bubblesData?.items?.length) {
      const preloaded = bubblesData.items
        .filter((item) => item.image_url)
        .map((item) => ({ nodeId: item.id, imageUrl: item.image_url }));
      if (preloaded.length) {
        chartRef.current.updateImages(preloaded);
      }
    }
  }, [bubblesData, containerDimensions, groupBy, loading, error, handleBubbleClick, handleBubbleHover, handleBubbleLeave]);

  // After artist bubbles load for a dataset/timeframe, prewarm the other views (album + historical)
  useEffect(() => {
    if (!dataReady || !bubblesData || loading || error) return;
    const baseKey = `${dataSource || 'unknown'}::${JSON.stringify(filters || {})}`;
    const otherKey = `${baseKey}::otherviews-prewarmed`;
    if (prewarmedKeysRef.current.has(otherKey)) return;

    prewarmedKeysRef.current.add(otherKey);
    (async () => {
      try {
        const otherGroup = groupBy === 'artist' ? 'album' : 'artist';
        const albumUrl = apiUrl('/api/bubbles', { ...bubbleParams, group_by: otherGroup });
        const histUrl = apiUrl('/api/historical_data', { ...filters, limit: 200 });
        const summaryUrl = apiUrl('/api/summary', { ...filters });
        console.time(`[prefetch] bubbles:${otherGroup}`);
        console.time('[prefetch] historical');
        console.time('[prefetch] summary');
        await Promise.all([
          fetchJSON(albumUrl).finally(() => console.timeEnd(`[prefetch] bubbles:${otherGroup}`)),
          fetchJSON(histUrl).finally(() => console.timeEnd('[prefetch] historical')),
          fetchJSON(summaryUrl).finally(() => console.timeEnd('[prefetch] summary')),
        ]);
      } catch (e) {
        // ignore
      }
    })();
  }, [dataReady, dataSource, groupBy, bubblesData, bubbleParams, filters, loading, error]);

  // Removed periodic alpha restarts to avoid visible stutters.

  useEffect(() => {
    const tooltipRef = tooltipStateRef;
    const chartHandle = chartRef;

    return () => {
      const state = tooltipRef.current;
      if (state?.rafId) {
        cancelAnimationFrame(state.rafId);
        state.rafId = null;
      }
      if (chartHandle.current) {
        chartHandle.current.destroy();
        chartHandle.current = null;
      }
    };
  }, []);

  useEffect(() => {
      // Clear selection when groupBy changes
      if (chartRef.current) {
        chartRef.current.clearSelection();
      }
      // Also clear details in the right panel
      onEntityDetails(null);
    }, [groupBy, onEntityDetails]);

  // Respond to external clear selection signal from parent
  useEffect(() => {
    if (clearSignal === undefined) return;
    if (chartRef.current) {
      chartRef.current.clearSelection();
    }
    onEntityDetails(null);
    handleBubbleLeave();
  }, [clearSignal, onEntityDetails, handleBubbleLeave]);

  const gateActive = !dataReady;
  const overlayActive = !gateActive && (loading || !!error);
  const tooltipVisible = tooltip.show && !overlayActive && !gateActive;
  const errorMessage = typeof error?.message === 'string' ? error.message : 'Something went wrong.';

  const overlayClassName = [
    'bubbles-container__overlay',
    loading && !error ? 'bubbles-container__overlay--loading' : '',
    error ? 'bubbles-container__overlay--error' : '',
  ].filter(Boolean).join(' ');

  const containerClasses = ['bubbles-container'];
  if (loading) containerClasses.push('is-loading');
  if (error) containerClasses.push('is-error');
  if (gateActive) containerClasses.push('is-gated');
  const containerClassName = containerClasses.join(' ');

  // Make tooltip position right under the cursor
  return (
    <>
      <div
        ref={containerRef}
        className={containerClassName}
        aria-busy={loading && !gateActive}
      >
        <svg ref={svgRef} className="bubbles-svg" aria-hidden={gateActive || overlayActive} />

        {gateActive && (
          <div className="bubbles-gate" role="dialog" aria-modal="true">
            <div className={`bubbles-gate__panel${uploading ? ' is-uploading' : ''}`}>
              <div
                className={`bubbles-gate__column bubbles-gate__column--drop${isDragActive ? ' is-active' : ''}`}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
              >
                <h2 className="bubbles-gate__title">Upload your Spotify data</h2>
                <button
                  type="button"
                  className="bubbles-gate__button"
                  onClick={handleBrowseClick}
                  disabled={uploading}
                >
                  {uploading ? 'Processing...' : 'Select a file'}
                </button>
              </div>

              <div className="bubbles-gate__divider" aria-hidden="true" />

              <div className="bubbles-gate__column bubbles-gate__column--sample">
                <h2 className="bubbles-gate__title">Test with my data!</h2>
                <button
                  type="button"
                  className="bubbles-gate__button bubbles-gate__button--ghost"
                  onClick={handleUseDefault}
                  disabled={uploading}
                >
                  {uploading ? 'Loading...' : 'Load demo'}
                </button>
              </div>
            </div>
            {uploadError && (
              <p className="bubbles-gate__error" role="alert">{uploadError}</p>
            )}
          </div>
        )}

        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.json,application/json"
          style={{ display: 'none' }}
          onChange={handleFileInputChange}
        />

        {overlayActive && (
          <div className={overlayClassName} role={error ? 'alert' : undefined}>
            {loading && <div className="loading-spinner" />}
            {error && (
              <div className="error-message">
                <p className="error-title">Error Loading Data</p>
                <p className="error-text">{errorMessage}</p>
              </div>
            )}
          </div>
        )}
      </div>
      <Tooltip show={tooltipVisible} x={tooltip.x} y={tooltip.y} className="tooltip">
        <div dangerouslySetInnerHTML={{ __html: tooltip.content }}/>
      </Tooltip>
    </>
  );
};

export default BubblesView;
