# Track Graph — Spotify Listening Analytics

Track Graph turns your Spotify history into a polished, recruiter-friendly data experience. Upload your export (or use the bundled sample) and explore share of listening time by artist or album, complete with imagery, zoomable bubbles, and a curated leaderboard of your top artists.

The market for music data visualization is HUGE, places like stats.fm and last.fm have millions of paying users to do exactly what I do in the project within different UI environments. Services like Spotify Wrapped or Apply Replay also generate extensive buzz on the internet so my WebApp fills a genuine niche. The thing my app does differently is presenting the data in a more fun and interactive way. Both stats.fm and last.fm only have clean but boring ui's that mainly consist of top stats for each category, so I chose to build an app where fun interactive experiences with user data was the focal point.

## Feature Snapshot

- **Bubble View:** Force-directed D3 visualization with physics-aware dragging, focus zoom, and automatic resizing so bubbles never overlap while preserving relative listening time.
- **Leaderboard View:** Ranked artists, albums, and tracks with art, Spotify deep links, and a carousel of 98 runners-up.
- **Timeframe & Data Controls:** Date filters, artist/album grouping, upload gate with drag-and-drop, and a one-click sample dataset.
- **Performance & Observability:** Cached metadata lookups, API timings surfaced to the client, JSON structured logs, and Prometheus `/metrics`.

## Architecture Overview

```
Spotify Export (JSON/CSV)
        │
        ▼
FastAPI backend (backend/)
  • Data normalization, aggregation, caching
  • Upload & sample dataset endpoints
  • Batch hydration for Spotify imagery
        │
        ▼
React frontend (spotify-analytics-dashboard/)
  • Bubble view (D3)
  • Leaderboard view
  • Hooks/services for API access
        │
        ▼
CloudFront + S3 (static build) / App Runner (API)
```

### Backend at a Glance

- FastAPI with async endpoints for `/api/bubbles`, `/api/summary`, `/api/historical_data`.
- Upload handler reads CSV/JSON exports, anonymizes disallowed artists, and stores data in memory.
- Metadata cache (`static_data.json`) avoids redundant Spotify API calls; optionally mirrored to S3 via `SPOTIFY_HISTORY_S3_*` environment variables.
- Observability: `/metrics`, `/healthz`, structured logging, OpenTelemetry hooks for AWS X-Ray.

### Frontend at a Glance

- React + D3 rendering pipeline with persistent `<svg>` to keep physics stable between state changes.
- `useSpotifyData` hook layers caching and prefetching over the API, normalizing payloads for BubbleChart and LeaderboardChart.
- Responsive layout with CSS variables measured at runtime to maintain header/toolbar alignment.

## Data Pipeline & Privacy

- Supported inputs: Spotify `StreamingHistory*.json`, `endsong*.json`, and CSV exports containing standard columns in spotify historical data (`ts`, `ms_played`, metadata fields, shuffle/skipped flags).
- Personally identifiable data remains in-memory; uploads are not persisted to disk or external storage.
- S3 cache stores only public metadata (images, IDs).

## How to run it yourself!

### Prerequisites

- Python 3.11+
- Node.js 18+ and npm

### 1. Backend (FastAPI)

```bash
cd backend
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export SPOTIFY_HISTORY_PATH=../demo_data/cleaned_streaming_history.csv   # PowerShell: setx SPOTIFY_HISTORY_PATH ..\demo_data\cleaned_streaming_history.csv
uvicorn main:app --reload --port 8000
```

### 2. Frontend (React)

```bash
cd spotify-analytics-dashboard
npm install
export REACT_APP_API_BASE=http://127.0.0.1:8000
npm start
```

Visit http://localhost:3000 to interact with the dashboard.

### 3. Load Data

- Use the “Use sample data” button once the backend is pointed at the demo dataset (see below), or
- Upload a Spotify export (`StreamingHistory*.json`, `endsong*.json`, or compatible CSV). The backend normalizes the payload and exposes it via the API.

## Demo Dataset

- `demo_data/cleaned_streaming_history.csv` contains the historical data of my partner and I so you can demo it.
- Frontend prewarms this dataset so recruiters can explore the dashboard instantly without uploading their own files.

## Testing

- **Frontend unit tests:** `npm run test --prefix spotify-analytics-dashboard -- --watchAll=false`
- **Visualization checks:** custom scripts in `tests/` validate bubble scaling invariants.
- **Playwright E2E:** `npm run test:smoke`, `npm run test:perf`, `npm run test:playwright` (requires backend and frontend running).
- Planned backend API tests (pytest) will cover upload flows, aggregation endpoints, and batch hydration.

## Deployment

- **Backend:** Dockerfile in `backend/` supports App Runner deployment. Configure `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, `SPOTIFY_HISTORY_S3_BUCKET`, and `SPOTIFY_HISTORY_S3_KEY` for AWS integration.
- **Frontend:** `spotify-analytics-dashboard/Dockerfile` builds the React app with `REACT_APP_API_BASE`, then serves via nginx. For static hosting, sync the `build/` directory to S3 and serve through CloudFront.
- `deploy.sh` illustrates pushing the backend image to ECR and refreshing the frontend bucket.

## Project Structure

```
backend/                      FastAPI service, data processing, caching
spotify-analytics-dashboard/  React application + D3 visualizations
tests/                        Visualization/unit test utilities
deploy.sh                     Example deployment helper script
```

## Roadmap

- Shareable links encoding timeframe, grouping, and selected entity.
- Insights view with streaks, discovery vs. replays, and time-of-day heatmaps.
- IndexedDB persistence for user uploads to avoid re-imports.
- GitHub Actions workflows for lint, unit, API, and smoke E2E tests.
- Extended accessibility work: keyboard navigation and reduced-motion variants.

---

Questions or interest in a deeper walkthrough? Reach out at `josephsaldivarg@gmail.com`.
