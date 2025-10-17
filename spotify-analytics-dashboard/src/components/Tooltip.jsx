// src/components/Tooltip.jsx
import React from "react";
import { createPortal } from "react-dom";

export default function Tooltip({ show, x, y, children, className = "" }) {
  if (!show) return null;

  // Render to body with fixed positioning so it isn't clipped
  // by overflow/positioning of ancestor containers.
  return createPortal(
    <div
      role="tooltip"
      aria-hidden={!show}
      className={`tooltip ${className}`}
      style={{ left: x, top: y, position: "fixed" }}
    >
      {children}
    </div>,
    document.body
  );
}
