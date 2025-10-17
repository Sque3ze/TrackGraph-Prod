// src/config.js
const apiBase = process.env.REACT_APP_API_BASE || "http://127.0.0.1:8000";

export const CONFIG = {
  API_BASE: apiBase,
  COLORS: {
    primary: "#1ed760",
    secondary: "#1ed760",
    background: "#191414",
    surface: "rgba(255,255,255,0.1)",
    text: "#ffffff",
    textSecondary: "#b3b3b3",
  },
  BUBBLE_SETTINGS: {
    targetCoverage: 0.82,
    minRadius: 6,

    collisionPadding: 2,
    chargeStrengthMultiplier: 0.01,
    collisionIterations: 2,
    initialAlpha: 0.25,
    alphaDecay: 0.08,
    alphaTarget: 0,
    physicsEnabled: true,
    panSlackRatio: 0.35,
    minImageVisiblePx: 48,
    zoomCap: 120,
    maxFillRatio: 0.82,
    initialPlacementRatio: 0.78,
    focusTranslateSlackRatio: 0.25,
    focusZoomRange: [1.35, 3.6]
  },
  ANIMATION_SETTINGS: {
    transitionDuration: 700,
    bubbleTransition: 200,  // <-- Bubble hover animation timing used by BubbleChart
    zoomDuration: 750,
    shakeInterval: 4000,
  },
  LABEL_SETTINGS: {
    fontFactor: 0.35,
    minReadablePx: 12,
    maxZoom: 12,
  },
};
