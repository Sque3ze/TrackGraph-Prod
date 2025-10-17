// src/utils/formatting.js
export const formatHours = (h) => `${(h ?? 0).toFixed(2)}h`;
export const formatPercent = (p) => {
  const value = Number(p ?? 0);
  if (!Number.isFinite(value)) return "0.00%";
  return `${(value * 100).toFixed(2)}%`;
};
export const formatNumber = (n) =>
  (n ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 });

export const truncateText = (s, max = 16) =>
  (s || "").length > max ? `${s.slice(0, max - 1)}…` : s;

export const hashColor = (id = "") => {
  // tiny deterministic color for nodes
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) | 0;
  const hue = Math.abs(h) % 360;
  return `hsl(${hue},70%,45%)`;
};
