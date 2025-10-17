# Spotify Analytics Dashboard — Frontend

A React application that visualizes long-term Spotify listening history. It powers the public-facing experience for Track Graph, rendering an interactive bubble chart and leaderboard fed by my FastAPI backend in `../backend`.

## Highlights

- Bubble view that keeps D3 mounted between state changes, maintaining fluid zoom/drag interactions while toggling between artist and album groupings.
- Leaderboard view with a carousel of runners-up, hydrated with Spotify imagery through cached batch endpoints.
- Cohesive dark theme, responsive layout, and detail drawer that scales to the viewport without clipping content.
- Upload gate that accepts raw Spotify exports (JSON or CSV) and a sample-data path for quick demos.

## Tech Stack

- React 19 with hooks for state, layout, and data fetching.
- D3 v7 for bubble simulation, physics, and zoom orchestration.
- Custom data hooks (`useSpotifyData`) with a client-side cache and optimistic prefetching.
- CSS modules (global + feature styles) for layout; no utility framework/runtime CSS required.

## Project Structure

```
src/
  App.js                  // root layout + view orchestration
  components/             // React views (Bubbles, Leaderboard, Tooltip)
  hooks/useSpotifyData.js // API wiring, caching, and normalization
  services/api.js         // CONFIG-aware URL builder + fetch helper
  styles/                 // feature-level styles layered on top of index.css
  utils/                  // formatting helpers and bubble math
  visualizations/         // D3 + DOM renderers (BubbleChart, LeaderboardChart)
```

## Environment

- `REACT_APP_API_BASE` (optional): Base URL for the backend API. Defaults to `http://127.0.0.1:8000` for local development. Set this when pointing at a deployed backend (e.g., App Runner).

## Run it Locally

Run the backend first then do this:

1. Install dependencies:
   ```bash
   npm install
   ```
2. Provide the backend URL if it differs from the default:
   ```bash
   export REACT_APP_API_BASE=http://localhost:8000
   ```
3. Start the development server:
   ```bash
   npm start
   ```
   The app runs on `http://localhost:3000` and proxies API calls to `REACT_APP_API_BASE`.

## Testing Notes

- Jest is configured with custom D3 mocks in `src/setupTests.js` so tests can focus on React behavior without requiring the D3 runtime.
- Visualization logic (e.g., bubble sizing math) is validated through separate scripts in `../tests`.

## Deployment

- `Dockerfile` builds the React app with the desired `REACT_APP_API_BASE`, then serves the static bundle via nginx.
- `nginx.conf` provides SPA routing (`try_files ... /index.html`) and long-lived caching for hashed assets.
- Production deployments push the `build/` output to S3 and serve through CloudFront. Update the environment variable during build to target the deployed backend.

---

For more details see the repository root `README.md`.
