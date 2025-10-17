// visualizations/BubbleChart.js
// Pure D3 visualization logic - no React dependencies

import * as d3 from 'd3';
import { CONFIG } from '../config';
import { truncateText, hashColor } from '../utils/formatting';
import bubbleMath from '../utils/bubbleMath';

const { computeProportionalRadii } = bubbleMath;

export class BubbleChart {
  constructor(svgRef, options = {}) {
    this.svg = d3.select(svgRef.current)
      .style('overflow', 'visible');
    this.options = {
      ...CONFIG.BUBBLE_SETTINGS,
      focusZoom: 2.2,
      focusTargetXRatio: 0.35,
      focusTargetYRatio: 0.5,
      focusZoomDuration: 650,
      focusEdgePadding: 40,
      focusZoomEase: d3.easeCubicInOut,
      focusZoomMinDuration: 360,
      focusZoomMaxDuration: 1200,
      focusZoomScaleWeight: 0.9,
      focusZoomDistanceMultiplier: 1.2,
      debugZoom: false,
      ...options,
    };
    this.simulation = null;
    this.nodes = [];
    this.onBubbleClick = options.onBubbleClick || (() => {});
    this.onBubbleHover = options.onBubbleHover || (() => {});
    this.onBubbleLeave = options.onBubbleLeave || (() => {});
    this.currentTransform = d3.zoomIdentity;
    this.clipSel = null;

    this.reheatTimers = [];
    this.pendingReheat = false;
    this.pendingReheatAt = 0;
    this.velocitySettleThreshold = this.options.velocitySettleThreshold ?? 0.08;
    this.alphaSettleThreshold = this.options.alphaSettleThreshold ?? 0.12;
    this.lowVelocityTickCount = 0;
    this.reheatConsecutiveTicks = Math.max(1, this.options.reheatConsecutiveTicks ?? 3);
    this.maxPendingReheatDuration = Math.max(200, this.options.maxPendingReheatDuration ?? 900);

    this.isRedistributing = false;
    this.originalChargeStrength = null;
    this.redistributionTimer = null;
    this.bounds = null;
    this.boundingRadius = 0;
    this.draggedDuringInteraction = false;
    this.redistributionTimers = [];
    this._originalCollisionIterations = null;
    this.interactionCircle = null;
    this.baseTransform = d3.zoomIdentity;
    this.selectedBubbleId = null;
    this.zoom = null;
    this.gRoot = null;
    this.labelLayerGroup = null;
    this.labelVisibilityPx = CONFIG.LABEL_SETTINGS.minReadablePx ?? 12;
    this.silenceZoomEvents = false;
    this.zoomRafId = null;
    this.debugZoom = !!this.options.debugZoom;
    this.logZoom = (...args) => {
      if (this.debugZoom && typeof console !== 'undefined' && console.log) {
        console.log('[BubbleChart zoom]', ...args);
      }
    };

    // Mobile/perf mode heuristics (coarse pointer or small viewport)
    const isCoarsePointer = (typeof window !== 'undefined') && window.matchMedia && window.matchMedia('(pointer: coarse)').matches;
    this.isMobilePerfMode = !!isCoarsePointer;
    this.enableGradients = true; // set concretely per-render with real dimensions
  }

  render(data, dimensions) {
    if (this.simulation) {
      this.simulation.stop();
      this.simulation = null;
    }
    this.svg.on(".zoom", null); // Remove previous zoom handlers


    const { width, height } = dimensions;
    const previousState = {
      centerX: Number.isFinite(this.centerX) ? this.centerX : null,
      centerY: Number.isFinite(this.centerY) ? this.centerY : null,
      boundingRadius: Number.isFinite(this.boundingRadius) ? this.boundingRadius : null,
      nodes: Array.isArray(this.nodes)
        ? this.nodes.map((node) => ({
            id: node.id,
            x: Number.isFinite(node?.x) ? node.x : null,
            y: Number.isFinite(node?.y) ? node.y : null,
            vx: Number.isFinite(node?.vx) ? node.vx : 0,
            vy: Number.isFinite(node?.vy) ? node.vy : 0,
          }))
        : null,
    };

    this.svg.attr("viewBox", `0 0 ${width} ${height}`);

    // Re-evaluate performance mode by dimension as well (small screens)
    const smallViewport = Math.min(width || 0, height || 0) <= 520;
    this.isMobilePerfMode = this.isMobilePerfMode || smallViewport;
    this.enableGradients = !this.isMobilePerfMode;

    if (!data.items || data.items.length === 0) {
      this.renderEmptyState(width, height);
      return;
    }

     this.width = width;
     this.height = height;
     this.centerX = width / 2;
     this.centerY = height / 2;
     this.processNodes(data, width, height, previousState);   // compute radii + positions with the SAME dims

     if (!Array.isArray(this.nodes) || this.nodes.length === 0) {
       this.renderEmptyState(width, height);
       return;
     }

     this.setupCanvas(width, height);          // defs/gradients now see nodes
     this.setupVisualization(width, height);
  }

  processNodes(data, width, height, previousState = {}) {
    const items = Array.isArray(data.items) ? data.items : [];

    if (!items.length) {
      this.nodes = [];
      return;
    }

    const previousNodesArray = Array.isArray(previousState?.nodes) ? previousState.nodes : null;
    const previousNodesMap = previousNodesArray
      ? new Map(previousNodesArray.map((entry) => [entry.id, entry]))
      : null;
    const prevCenterX = Number.isFinite(previousState?.centerX) ? previousState.centerX : null;
    const prevCenterY = Number.isFinite(previousState?.centerY) ? previousState.centerY : null;
    const prevBoundingRadius = Number.isFinite(previousState?.boundingRadius) ? previousState.boundingRadius : null;

    const minDim = Math.max(1, Math.min(width, height));
    const padding = this.options.collisionPadding ?? 0;
    const boundingRadius = Math.max(10, (minDim / 2) - padding);
    const placementRatio = Number.isFinite(this.options.initialPlacementRatio)
      ? this.options.initialPlacementRatio
      : 0.78;
    const placementRadius = Math.max(10, boundingRadius * Math.min(0.98, Math.max(0.1, placementRatio)));
    const cx = width / 2;
    const cy = height / 2;

    const containerArea = Math.max(1, Math.min(width * height, Math.PI * boundingRadius * boundingRadius));
    const coverage = this.computeDynamicCoverage(items.length);
    const targetArea = Math.max(1, containerArea * coverage);

    const configuredMinRadius = Number.isFinite(this.options.minRadius) ? this.options.minRadius : 6;
    const minRadiusFloor = Math.max(1.8, configuredMinRadius);
    const feasibleMinRadius = Math.sqrt((targetArea / Math.max(items.length, 1)) / Math.PI) * 0.92;
    const effectiveMinRadius = Math.max(1.8, Math.min(minRadiusFloor, Number.isFinite(feasibleMinRadius) ? feasibleMinRadius : minRadiusFloor));

    const baseRadii = this.computeBaseRadii(items, targetArea);
    const radii = this.distributeRadii(baseRadii, targetArea, effectiveMinRadius, boundingRadius);

    const epsilon = 1e-6;
    const nodes = [];
    const shareFromNode = (node) => {
      const pct = Number.isFinite(node?.value_pct) ? node.value_pct : 0;
      return pct > 1 ? pct / 100 : pct;
    };

    radii.forEach((radius, index) => {
      if (!(radius > epsilon)) {
        return;
      }

      const item = items[index];
      const prev = previousNodesMap?.get(item.id);
      const basePrevCx = Number.isFinite(prevCenterX) ? prevCenterX : cx;
      const basePrevCy = Number.isFinite(prevCenterY) ? prevCenterY : cy;
      const clampPadding = this.options.collisionPadding ?? 0;

      let x;
      let y;

      if (prev && Number.isFinite(prev.x) && Number.isFinite(prev.y)) {
        const scaleFactor = prevBoundingRadius && prevBoundingRadius > 0
          ? Math.min(1.25, boundingRadius / prevBoundingRadius)
          : 1;
        const dx = (prev.x - basePrevCx) * scaleFactor;
        const dy = (prev.y - basePrevCy) * scaleFactor;
        x = cx + dx;
        y = cy + dy;

        const maxDist = Math.max(0, (boundingRadius - radius) - clampPadding);
        const dist = Math.hypot(x - cx, y - cy);
        if (dist > maxDist && dist > 0) {
          const clamp = maxDist / dist;
          x = cx + dx * clamp;
          y = cy + dy * clamp;
        }

        const jitter = radius * 0.015;
        if (jitter > 0) {
          x += (Math.random() - 0.5) * jitter;
          y += (Math.random() - 0.5) * jitter;
        }
      }

      if (!Number.isFinite(x) || !Number.isFinite(y)) {
        const [seedX, seedY] = this.randomPointInCircle(cx, cy, placementRadius);
        x = seedX;
        y = seedY;
      }

      const velocityDamping = 0.78;
      const vx = prev && Number.isFinite(prev.vx) ? prev.vx * velocityDamping : 0;
      const vy = prev && Number.isFinite(prev.vy) ? prev.vy * velocityDamping : 0;

      nodes.push({
        ...item,
        r: radius,
        x,
        y,
        color: hashColor(item.id),
        vx,
        vy,
        image: item.image_url || item.image || null,
      });
    });

    this.nodes = nodes;

    const minSizedNodes = nodes.filter((node) => node.r <= effectiveMinRadius + epsilon);
    const scaledNodes = nodes.filter((node) => node.r > effectiveMinRadius + epsilon);

    let thresholdShare = 0;
    if (minSizedNodes.length) {
      thresholdShare = Math.max(...minSizedNodes.map(shareFromNode));
    } else if (scaledNodes.length) {
      thresholdShare = Math.min(...scaledNodes.map(shareFromNode));
    }

    this.dynamicThresholdShare = thresholdShare;
    this.dynamicScaleFactor = 1;
  }

  computeDynamicCoverage(count) {
    const base = Number.isFinite(this.options.targetCoverage)
      ? this.options.targetCoverage
      : (CONFIG.BUBBLE_SETTINGS?.targetCoverage ?? 0.85);
    const maxFill = Number.isFinite(this.options.maxFillRatio)
      ? this.options.maxFillRatio
      : 0.88;
    const minFill = 0.45;

    if (!Number.isFinite(count) || count <= 0) {
      return Math.max(minFill, Math.min(maxFill, base));
    }

    const densePenalty = count > 40 ? Math.min(0.28, (count - 40) * 0.0025) : 0;
    const sparseBoost = count < 14 ? Math.min(0.12, (14 - count) * 0.008) : 0;

    const adjusted = base + sparseBoost - densePenalty;
    return Math.max(minFill, Math.min(maxFill, adjusted));
  }

  computeBaseRadii(items, targetArea) {
    if (!Array.isArray(items) || !items.length || !(targetArea > 0)) {
      return Array.isArray(items) ? items.map(() => 0) : [];
    }

    let radii;
    try {
      radii = computeProportionalRadii(items, targetArea);
    } catch (err) {
      radii = null;
    }

    if (!Array.isArray(radii) || radii.length !== items.length) {
      return items.map(() => 0);
    }

    return radii.map((r) => (Number.isFinite(r) && r > 0 ? r : 0));
  }

  distributeRadii(baseRadii, targetArea, minRadius, boundingRadius) {
    if (!Array.isArray(baseRadii) || !baseRadii.length || !(targetArea > 0)) {
      return Array.isArray(baseRadii) ? baseRadii.map(() => 0) : [];
    }

    const minArea = Math.PI * Math.max(0, minRadius) * Math.max(0, minRadius);
    const baseAreas = baseRadii.map((r) => (r > 0 ? Math.PI * r * r : 0));
    const finalAreas = baseAreas.map(() => 0);
    const positiveIndices = baseAreas
      .map((area, index) => ({ area, index }))
      .filter(({ area }) => area > 0);

    if (!positiveIndices.length) {
      return finalAreas.map(() => 0);
    }

    let remainingArea = targetArea;
    const unlocked = new Set(positiveIndices.map(({ index }) => index));
    const maxIterations = 12;

    for (let iteration = 0; iteration < maxIterations; iteration += 1) {
      if (!unlocked.size) {
        break;
      }

      let unlockedBaseArea = 0;
      for (const idx of unlocked) {
        unlockedBaseArea += baseAreas[idx];
      }

      if (!(unlockedBaseArea > 0)) {
        break;
      }

      const allocationScale = remainingArea / unlockedBaseArea;
      let lockedThisPass = false;

      // Iterate over a snapshot of indices to allow safe mutation of the Set
      for (const idx of Array.from(unlocked)) {
        if (lockedThisPass) break;
        const proposedArea = baseAreas[idx] * allocationScale;
        if (proposedArea < minArea) {
          finalAreas[idx] = minArea;
          remainingArea = Math.max(0, remainingArea - minArea);
          unlocked.delete(idx);
          lockedThisPass = true;
        }
      }

      if (!lockedThisPass) {
        for (const idx of unlocked) {
          finalAreas[idx] = Math.max(0, baseAreas[idx] * allocationScale);
        }
        remainingArea = 0;
        break;
      }
    }

    if (remainingArea > 0 && unlocked.size) {
      let unlockedBaseArea = 0;
      for (const idx of unlocked) {
        unlockedBaseArea += baseAreas[idx];
      }
      const allocationScale = unlockedBaseArea > 0 ? (remainingArea / unlockedBaseArea) : 0;
      for (const idx of unlocked) {
        finalAreas[idx] = Math.max(0, baseAreas[idx] * allocationScale);
      }
      remainingArea = 0;
    }

    let radii = finalAreas.map((area, index) => {
      if (area > 0) {
        return Math.sqrt(area / Math.PI);
      }
      const base = baseRadii[index];
      return base > 0 ? base : 0;
    });

    let totalArea = radii.reduce((sum, r) => sum + Math.PI * r * r, 0);
    if (totalArea > targetArea && totalArea > 0) {
      const shrink = Math.sqrt(targetArea / totalArea);
      radii = radii.map((r) => r * shrink);
      totalArea = targetArea;
    }

    const maxRadius = Math.max(...radii);
    if (boundingRadius > 0 && maxRadius > boundingRadius) {
      const shrink = boundingRadius / maxRadius;
      radii = radii.map((r) => r * shrink);
      totalArea = radii.reduce((sum, r) => sum + Math.PI * r * r, 0);
    }

    if (targetArea > 0 && totalArea > 0 && totalArea < targetArea * 0.82) {
      const grow = Math.sqrt(targetArea / totalArea);
      const currentMax = Math.max(...radii);
      const boundScale = boundingRadius > 0 && currentMax > 0
        ? Math.min(1, (boundingRadius * 0.98) / currentMax)
        : 1;
      const scale = Math.min(grow, boundScale > 0 ? boundScale : grow);
      if (scale > 1.01) {
        radii = radii.map((r) => r * scale);
      }
    }

    return radii.map((r) => (Number.isFinite(r) && r > 0 ? r : 0));
  }

  computeFitViewTransform(width, height) {
    // Keep the baseline transform stable so the viewport position never
    // depends on the current bubble distribution.
    if (!(width > 0) || !(height > 0)) {
      return d3.zoomIdentity;
    }

    return d3.zoomIdentity;
  }

  setupCanvas(width, height) {
    // Clear and setup defs, clips, etc.
    this.svg.selectAll("*").remove();
    this.svg.attr("xmlns:xlink", "http://www.w3.org/1999/xlink");

    const defs = this.svg.append("defs");

    // Single reusable circular clip for all images using objectBoundingBox
    // Avoids per-node clipPath updates on every tick.
    defs.append('clipPath')
      .attr('id', 'clip-node-circle')
      .attr('clipPathUnits', 'objectBoundingBox')
      .append('circle')
      .attr('cx', 0.5)
      .attr('cy', 0.5)
      .attr('r', 0.5);
    this.clipSel = null; // no per-node clip updates needed

    // Optional gradients (skip on mobile/perf mode)
    if (this.enableGradients) {
      this.nodes.forEach((d, i) => {
        if (d.image) return; // Skip gradients for nodes rendered with album art
        const gradient = defs.append("radialGradient")
          .attr("class", "node-gradient")
          .attr("id", `gradient-${i}`)
          .attr("cx", "30%")
          .attr("cy", "30%");
        gradient.append("stop")
          .attr("offset", "0%")
          .attr("stop-color", d3.color(d.color).brighter(0.5));
        gradient.append("stop")
          .attr("offset", "100%")
          .attr("stop-color", d.color);
      });
    }
  }

  setupVisualization(width, height) {
    const cx = width / 2;
    const cy = height / 2;
    const padding = this.options.collisionPadding;
    const R = Math.max(0.1, Math.min(width, height) / 2 - padding);

    this.interactionCircle = { cx, cy, radius: R };

    // Create layers
    const gRoot = this.svg.append("g");
    const imgLayer = gRoot.append("g").attr("class", "img-layer");
    const circleLayer = gRoot.append("g").attr("class", "circle-layer");
    const labelLayer = gRoot.append("g").attr("class", "label-layer");

    this.gRoot = gRoot;
    this.labelLayerGroup = labelLayer;

    // Determine zoom extents dynamically so small bubbles remain inspectable
    const labelVisibilityPx = Number.isFinite(this.options.labelVisibilityPx)
      ? this.options.labelVisibilityPx
      : (CONFIG.LABEL_SETTINGS.minReadablePx ?? 12);
    const labelFontFactor = CONFIG.LABEL_SETTINGS.fontFactor || 0.35;
    const minNodeRadius = d3.min(this.nodes, d => d.r) || Math.max(0.5, this.options.minRadius ?? 0.5);
    const safeRadius = Math.max(minNodeRadius, 0.5);
    const requiredLabelZoom = labelVisibilityPx / Math.max(safeRadius * labelFontFactor, 0.001);
    const minImageVisiblePx = Number.isFinite(this.options.minImageVisiblePx)
      ? this.options.minImageVisiblePx
      : labelVisibilityPx * 4;
    const requiredImageZoom = minImageVisiblePx / Math.max(safeRadius * 2, 0.001);
    const baseMaxZoom = CONFIG.LABEL_SETTINGS.maxZoom || 12;
    const desiredMaxZoom = Math.max(1, baseMaxZoom, requiredLabelZoom, requiredImageZoom);
    const zoomCap = Number.isFinite(this.options.zoomCap) ? this.options.zoomCap : 120;
    const maxZoom = Math.min(zoomCap, desiredMaxZoom);
    const focusRange = Array.isArray(this.options.focusZoomRange)
      ? [...this.options.focusZoomRange]
      : [1.35, 3.6];
    const focusMin = Number.isFinite(focusRange[0]) ? focusRange[0] : 1.35;
    const focusMax = Number.isFinite(focusRange[1]) ? focusRange[1] : 3.6;
    const updatedMaxFocus = Math.min(maxZoom, Math.max(focusMax, desiredMaxZoom));
    const updatedMinFocus = Math.min(Math.max(1, focusMin), updatedMaxFocus);
    this.options.focusZoomRange = [updatedMinFocus, updatedMaxFocus];
    this.maxZoom = maxZoom;
    this.labelVisibilityPx = labelVisibilityPx;

    const fitView = this.computeFitViewTransform(width, height);
    const minZoom = 1;

    // Setup zoom with bounded pan/zoom extent
    const panSlack = Math.max(width, height) * (this.options.panSlackRatio ?? 0.35);
    const translateExtent = [[-panSlack, -panSlack], [width + panSlack, height + panSlack]];
    this.zoom = d3.zoom()
      .scaleExtent([minZoom, maxZoom])
      .translateExtent(translateExtent)
      .extent([[0, 0], [width, height]])
      .filter(this.shouldHandleZoomEvent.bind(this))
      .on("zoom", (event) => {
        if (this.silenceZoomEvents) {
          this.logZoom('zoom handler silenced');
          return;
        }
        const sourceEvent = event?.sourceEvent;
        const sourceType = sourceEvent?.type || null;
        const userTypes = new Set(['wheel', 'mousedown', 'mousemove', 'mouseup', 'pointerdown', 'pointermove', 'pointerup', 'touchstart', 'touchmove', 'touchend']);
        const isUserGesture = !!sourceEvent && (userTypes.has(sourceType) || (sourceType && sourceType.startsWith('touch')));
        this.logZoom('zoom handler', {
          sourceType,
          isUserGesture,
          transform: {
            x: event.transform?.x,
            y: event.transform?.y,
            k: event.transform?.k
          }
        });
        if (isUserGesture) {
          this.stopActiveZoomAnimation();
        }
        this.applyZoomTransform(event.transform, { skipBehaviorSync: true });
      });

    this.svg
      .call(this.zoom)
      .on("dblclick.zoom", null);

    this.baseTransform = fitView;
    this.applyZoomTransform(fitView, { skipBehaviorSync: true, allowSilence: true });
    this.syncZoomBehavior(fitView);

    // Create elements
    this.imgSel = this.createImages(imgLayer);
    this.circleSel = this.createCircles(circleLayer);
    this.labelSel = this.createLabels(labelLayer);

    // Setup physics if enabled
    if (this.options.physicsEnabled !== false) {
      this.setupPhysics(circleLayer, labelLayer, width, height, { cx, cy, R });
    }

    this.reapplySelection({ animate: false });
  }

  normalizeTransform(transform) {
    if (!transform) return d3.zoomIdentity;
    const base = this.baseTransform || d3.zoomIdentity;
    const epsilon = 1e-4;
    const tx = Number.isFinite(transform.x) ? transform.x : 0;
    const ty = Number.isFinite(transform.y) ? transform.y : 0;
    const tk = Number.isFinite(transform.k) ? transform.k : 1;

    const bx = Number.isFinite(base.x) ? base.x : 0;
    const by = Number.isFinite(base.y) ? base.y : 0;
    const bk = Number.isFinite(base.k) ? base.k : 1;

    if (
      Math.abs(tx - bx) <= epsilon &&
      Math.abs(ty - by) <= epsilon &&
      Math.abs(tk - bk) <= epsilon
    ) {
      return d3.zoomIdentity.translate(bx, by).scale(bk);
    }
    return d3.zoomIdentity.translate(tx, ty).scale(tk);
  }

  applyZoomTransform(transform, { skipBehaviorSync = false, allowSilence = false } = {}) {
    const normalized = this.normalizeTransform(transform);
    this.currentTransform = normalized;
    this.logZoom('applyZoomTransform', {
      x: normalized.x,
      y: normalized.y,
      k: normalized.k,
      skipBehaviorSync,
      allowSilence
    });

    if (this.gRoot) {
      this.gRoot.attr("transform", normalized);
    }

    const scale = Number.isFinite(normalized?.k) ? normalized.k : 1;
    if (this.labelLayerGroup) {
      this.labelLayerGroup.selectAll("text")
        .style("display", d => (d.r * CONFIG.LABEL_SETTINGS.fontFactor * scale) >= this.labelVisibilityPx ? null : "none");
    }

    if (!skipBehaviorSync && this.zoom && this.svg) {
      if (allowSilence) {
        this.silenceZoomEvents = true;
      }
      this.zoom.transform(this.svg, normalized);
      if (allowSilence) {
        this.silenceZoomEvents = false;
      }
    }
  }

  syncZoomBehavior(transform) {
    if (!this.zoom || !this.svg) return;
    this.silenceZoomEvents = true;
    this.logZoom('syncZoomBehavior', {
      x: transform?.x,
      y: transform?.y,
      k: transform?.k
    });
    this.zoom.transform(this.svg, transform);
    this.silenceZoomEvents = false;
  }

  stopActiveZoomAnimation() {
    if (this.zoomRafId !== null) {
      if (typeof cancelAnimationFrame === 'function') {
        cancelAnimationFrame(this.zoomRafId);
      }
      this.logZoom('cancel zoom raf');
      this.zoomRafId = null;
    }
    else {
      this.logZoom('stopActiveZoomAnimation idle');
    }
    if (this.svg) {
      this.svg.interrupt("bubble-zoom");
    }
  }

  computeZoomAnimationDuration(startTransform, endTransform) {
    const base = Math.max(0, this.options.focusZoomDuration || 650);
    const minDuration = Math.max(120, Number.isFinite(this.options.focusZoomMinDuration) ? this.options.focusZoomMinDuration : Math.round(base * 0.75));
    const maxDuration = Math.max(minDuration, Number.isFinite(this.options.focusZoomMaxDuration) ? this.options.focusZoomMaxDuration : Math.round(base * 1.65));
    const scaleWeight = Number.isFinite(this.options.focusZoomScaleWeight) ? this.options.focusZoomScaleWeight : 0.9;
    const distanceMultiplier = Number.isFinite(this.options.focusZoomDistanceMultiplier) ? this.options.focusZoomDistanceMultiplier : 1.2;

    const sx = Number.isFinite(startTransform?.x) ? startTransform.x : 0;
    const sy = Number.isFinite(startTransform?.y) ? startTransform.y : 0;
    const sk = Number.isFinite(startTransform?.k) ? Math.max(1e-6, startTransform.k) : 1;

    const ex = Number.isFinite(endTransform?.x) ? endTransform.x : 0;
    const ey = Number.isFinite(endTransform?.y) ? endTransform.y : 0;
    const ek = Number.isFinite(endTransform?.k) ? Math.max(1e-6, endTransform.k) : 1;

    const translateDistance = Math.hypot(ex - sx, ey - sy);
    const maxDim = Math.max(1, this.width || 0, this.height || 0);
    const normalizedTranslate = translateDistance / maxDim;
    const scaleDelta = Math.abs(ek - sk);

    const combinedChange = (normalizedTranslate * distanceMultiplier) + (scaleDelta * scaleWeight);
    const normalizedChange = Math.max(0, combinedChange);
    const duration = base * (0.75 + normalizedChange);
    return Math.max(minDuration, Math.min(maxDuration, duration));
  }

  animateToTransform(targetTransform, { duration } = {}) {
    if (!targetTransform || !this.svg || !this.zoom) {
      return;
    }

    const finalTransform = this.normalizeTransform(targetTransform);
    const startTransform = this.currentTransform || this.baseTransform || d3.zoomIdentity;
    const ms = Number.isFinite(duration) ? duration : this.computeZoomAnimationDuration(startTransform, finalTransform);
    const ease = this.options.focusZoomEase || d3.easeCubicInOut;
    const sx = Number.isFinite(startTransform.x) ? startTransform.x : 0;
    const sy = Number.isFinite(startTransform.y) ? startTransform.y : 0;
    const sk = Number.isFinite(startTransform.k) ? startTransform.k : 1;
    const ex = Number.isFinite(finalTransform.x) ? finalTransform.x : 0;
    const ey = Number.isFinite(finalTransform.y) ? finalTransform.y : 0;
    const ek = Number.isFinite(finalTransform.k) ? finalTransform.k : 1;
    const startTime = (typeof performance !== 'undefined' && typeof performance.now === 'function')
      ? performance.now()
      : Date.now();

    this.stopActiveZoomAnimation();
    this.logZoom('animateToTransform start', {
      fromState: { x: sx, y: sy, k: sk },
      toState: { x: ex, y: ey, k: ek },
      duration: ms
    });

    if (!(ms > 0) || typeof requestAnimationFrame !== 'function') {
      this.applyZoomTransform(finalTransform, { skipBehaviorSync: true });
      this.syncZoomBehavior(finalTransform);
      return;
    }

    const runFrame = () => {
      const now = (typeof performance !== 'undefined' && typeof performance.now === 'function')
        ? performance.now()
        : Date.now();
      const elapsed = now - startTime;
      const tRaw = ms > 0 ? Math.min(1, Math.max(0, elapsed / ms)) : 1;
      const t = ease(Math.min(1, Math.max(0, tRaw)));
      const x = sx + (ex - sx) * t;
      const y = sy + (ey - sy) * t;
      const k = sk + (ek - sk) * t;
      const tweenTransform = d3.zoomIdentity.translate(x, y).scale(k);
      this.applyZoomTransform(tweenTransform, { skipBehaviorSync: true });

      if (elapsed >= ms) {
        this.logZoom('animateToTransform complete');
        this.zoomRafId = null;
        this.applyZoomTransform(finalTransform, { skipBehaviorSync: true });
        this.syncZoomBehavior(finalTransform);
        return;
      }

      this.zoomRafId = requestAnimationFrame(runFrame);
    };

    this.zoomRafId = requestAnimationFrame(runFrame);
  }

  shouldHandleZoomEvent(event) {
    if (!event) return false;

    const defaultAllowed = ((!event.ctrlKey || event.type === 'wheel') && !event.button);
    if (!defaultAllowed) {
      return false;
    }

    const eventType = event.type;
    const requiresPointerCheck = eventType === 'wheel'
      || eventType === 'mousedown'
      || eventType === 'pointerdown'
      || eventType === 'touchstart';

    if (!requiresPointerCheck) {
      return true;
    }

    if (!this.isPointerInsideInteractionCircle(event)) {
      return false;
    }

    if ((eventType === 'mousedown' || eventType === 'pointerdown' || eventType === 'touchstart') && this.isAtBaseZoom()) {
      return false;
    }

    return true;
  }

  isPointerInsideInteractionCircle(event) {
    if (!this.interactionCircle) {
      return true;
    }

    const svgNode = this.svg?.node();
    if (!svgNode) {
      return true;
    }

    const padding = Number.isFinite(this.options.interactionPadding) ? this.options.interactionPadding : 8;
    const radius = this.interactionCircle.radius + padding;
    const radiusSquared = radius * radius;

    const pointerSets = event.touches ? d3.pointers(event, svgNode) : [d3.pointer(event, svgNode)];
    for (const [px, py] of pointerSets) {
      if (!Number.isFinite(px) || !Number.isFinite(py)) {
        continue;
      }
      const dx = px - this.interactionCircle.cx;
      const dy = py - this.interactionCircle.cy;
      if ((dx * dx + dy * dy) <= radiusSquared) {
        return true;
      }
    }

    return false;
  }

  isAtBaseZoom(epsilon = 1e-3) {
    if (!this.currentTransform) {
      return true;
    }
    const { k } = this.currentTransform;
    const base = this.baseTransform || d3.zoomIdentity;
    const baseScale = Number.isFinite(base?.k) ? base.k : 1;
    return Math.abs((k || 1) - baseScale) <= epsilon;
  }

  createCircles(layer) {
    const circles = layer.selectAll("circle")
      .data(this.nodes, d => d.id)
      .join("circle")
      .attr("class", "bubble-node")
      .attr("r", d => d.r)
      .attr("cx", d => d.x)
      .attr("cy", d => d.y)
      .attr("fill", (d, i) => {
        if (d.image) return "none";
        return this.enableGradients ? `url(#gradient-${i})` : d.color;
      })
      .attr("stroke", "rgba(255, 255, 255, 0.3)")
      .attr("stroke-width", 1)
      .attr("shape-rendering", "optimizeSpeed")
      // Ensure the circle captures pointer events even when fill is 'none'
      .style("pointer-events", "all")
      .style("cursor", "pointer")
      .style("filter", "none")
      .on("mouseenter", (event, d) => { if (!this.isMobilePerfMode) this.handleMouseEnter(event, d); })
      .on("mousemove", (event, d) => { if (!this.isMobilePerfMode) this.handleMouseMove(event, d); })
      .on("mouseleave", (event, d) => { if (!this.isMobilePerfMode) this.handleMouseLeave(event, d); })
      .on("click", (event, d) => {
        event.stopPropagation();
        this.onBubbleClick(d);
      });

    // Setup drag
    const drag = d3.drag()
      .on("start", this.dragStart.bind(this))
      .on("drag", this.dragMove.bind(this))
      .on("end", this.dragEnd.bind(this));

    circles.call(drag);
    return circles;
  }

  createLabels(layer) {
    const sel = layer.selectAll("text")
      .data(this.nodes, d => d.id)
      .join("text")
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "central")
      .attr("pointer-events", "none")
      .attr("font-size", d => d.r * CONFIG.LABEL_SETTINGS.fontFactor)
      .attr("font-weight", "600")
      .attr("x", d => d.x)
      .attr("y", d => d.y)
      .attr("fill", "white");

    if (!this.isMobilePerfMode) {
      sel.style("text-shadow", "0 2px 4px rgba(0,0,0,0.8)");
    }

    sel.text(d => truncateText(d.label, 16));
    return sel;
  }

  createImages(layer) {
    return layer.selectAll("image.node-image")
      .data(this.nodes, d => d.id)
      .join("image")
      .attr("class", "node-image")
      .attr("x", d => d.x - d.r)
      .attr("y", d => d.y - d.r)
      .attr("width", d => d.r * 2)
      .attr("height", d => d.r * 2)
      .attr("preserveAspectRatio", "xMidYMid slice")
      .attr("clip-path", () => `url(#clip-node-circle)`)
      .attr("href", d => d.image || null)
      .style("pointer-events", "none")
      .style("display", d => d.image ? null : "none");
  }

  setupPhysics(circleLayer, labelLayer, width, height, bounds) {
    const avgRadius = d3.mean(this.nodes, d => d.r) || 1;
    const chargeStrength = -Math.pow(avgRadius, 1.2) * this.options.chargeStrengthMultiplier;
    const alphaTarget = typeof this.options.alphaTarget === 'number' ? this.options.alphaTarget : 0;

    this.originalChargeStrength = chargeStrength;
    this.bounds = bounds;
    this.boundingRadius = bounds?.R ?? Math.max(1, Math.min(width, height) / 2);
    this.centerX = bounds?.cx ?? this.centerX ?? width / 2;
    this.centerY = bounds?.cy ?? this.centerY ?? height / 2;

    const perfMode = !!this.isMobilePerfMode;
    const velocityDecay = perfMode ? 0.5 : 0.4;
    const alphaDecay = perfMode ? 0.08 : (this.options.alphaDecay || 0.05);
    const collisionIterations = this.options.collisionIterations || 1; // keep low for perf
    const collisionForce = d3.forceCollide()
      .radius(d => d.r + this.options.collisionPadding)
      .iterations(collisionIterations);

    this._originalCollisionIterations = typeof collisionForce.iterations === 'function'
      ? collisionForce.iterations()
      : (this.options.collisionIterations || 1);

    this.simulation = d3.forceSimulation(this.nodes)
      .force("charge", d3.forceManyBody().strength(chargeStrength))
      .force("collision", collisionForce)
      .velocityDecay(velocityDecay)
      .alpha(this.options.initialAlpha || 0.2)
      .alphaDecay(alphaDecay)
      .alphaMin(0.001)
      .alphaTarget(alphaTarget)
      .on("tick", () => this.onTick(circleLayer, labelLayer, bounds));
  }

  onTick(circleLayer, labelLayer, bounds) {
    // Constrain to circular boundary
    const { cx, cy, R } = bounds;
    const gap = this.options.collisionPadding;

    const circleSel = this.circleSel || circleLayer.selectAll("circle");
    circleSel
      .attr("cx", d => {
        const dx = d.x - cx;
        const dy = d.y - cy;
        const dist = Math.hypot(dx, dy);
        const maxDist = Math.max(0, R - d.r - gap);
        
        if (dist > maxDist) {
          const scale = maxDist / dist;
          d.x = cx + dx * scale;
          d.y = cy + dy * scale;
        }
        return d.x;
      })
      .attr("cy", d => d.y);

    const labelSel = this.labelSel || labelLayer.selectAll("text");
    labelSel.attr("x", d => d.x).attr("y", d => d.y);

    // Update images to follow circle positions
    this.imgSel.attr("x", d => d.x - d.r).attr("y", d => d.y - d.r);

    // No per-node clip updates needed with objectBoundingBox clip path

    if (this.pendingReheat && this.simulation) {
      const velocityThreshold = this.velocitySettleThreshold ?? 0.08;
      const alphaThreshold = this.alphaSettleThreshold ?? 0.12;
      const now = (typeof performance !== 'undefined' && typeof performance.now === 'function')
        ? performance.now()
        : Date.now();
      const maxVelocity = d3.max(this.nodes, node => {
        const vx = Number.isFinite(node.vx) ? node.vx : 0;
        const vy = Number.isFinite(node.vy) ? node.vy : 0;
        return Math.max(Math.abs(vx), Math.abs(vy));
      }) || 0;

      if (maxVelocity <= velocityThreshold) {
        this.lowVelocityTickCount += 1;
      } else {
        this.lowVelocityTickCount = 0;
      }

      const alphaReady = this.simulation.alpha() <= alphaThreshold;
      const velocityReady = this.lowVelocityTickCount >= this.reheatConsecutiveTicks;
      const timedOut = now - (this.pendingReheatAt || now) >= this.maxPendingReheatDuration;

      if ((alphaReady && velocityReady) || (alphaReady && timedOut)) {
        this.pendingReheat = false;
        this.lowVelocityTickCount = 0;
        this.pendingReheatAt = 0;
        this.triggerSmoothReheat();
      }
    }
  }

  // Event handlers that call React callbacks
  handleMouseEnter(event, d) {
    if (!this.isMobilePerfMode) {
      d3.select(event.target)
        .transition()
        .duration(CONFIG.ANIMATION_SETTINGS.bubbleTransition)
        .attr("stroke-width", 2)
        .attr("stroke", CONFIG.COLORS.primary)
        .style("filter", "drop-shadow(0 8px 16px rgba(0,0,0,0.4))");
    }

    this.onBubbleHover(event, d);
  }

  handleMouseLeave(event, d) {
    if (!this.isMobilePerfMode) {
      d3.select(event.target)
        .transition()
        .duration(CONFIG.ANIMATION_SETTINGS.bubbleTransition)
        .attr("stroke-width", 1)
        .attr("stroke", "rgba(255, 255, 255, 0.3)")
        .style("filter", "none");
    }

    this.onBubbleLeave(event, d);
  }

  handleMouseMove(event, d) {
    this.onBubbleHover(event, d);
  }

  // Utility methods
  randomPointInCircle(cx, cy, R) {
    const angle = 2 * Math.PI * Math.random();
    const radius = Math.sqrt(Math.random()) * (R * 0.8);
    return [cx + radius * Math.cos(angle), cy + radius * Math.sin(angle)];
  }

  computeCoverageMetrics() {
    const fallbackRadius = Math.min(this.width || 0, this.height || 0) / 2;
    const radius = this.boundingRadius || (this.bounds && this.bounds.R) || Math.max(1, fallbackRadius);
    if (!Number.isFinite(radius) || radius <= 0) {
      return { coverage: 0, emptyRatio: 1 };
    }

    const containerArea = Math.PI * radius * radius;
    if (!Number.isFinite(containerArea) || containerArea <= 0) {
      return { coverage: 0, emptyRatio: 1 };
    }

    const totalBubbleArea = d3.sum(this.nodes, node => Math.PI * node.r * node.r) || 0;
    const coverage = Math.max(0, Math.min(1, totalBubbleArea / containerArea));
    return { coverage, emptyRatio: Math.max(0, 1 - coverage) };
  }

  clearReheatTimers() {
    if (this.reheatTimers && this.reheatTimers.length) {
      this.reheatTimers.forEach(clearTimeout);
    }
    this.reheatTimers = [];
  }

  clearRedistributionAnimations() {
    if (this.redistributionTimers && this.redistributionTimers.length) {
      this.redistributionTimers.forEach(handle => {
        if (!handle) return;
        if (typeof handle.stop === 'function') {
          handle.stop();
        } else {
          clearTimeout(handle);
        }
      });
    }
    this.redistributionTimers = [];

    if (this.redistributionTimer) {
      clearTimeout(this.redistributionTimer);
      this.redistributionTimer = null;
    }
  }

  triggerSmoothReheat() {
    if (!this.simulation) return;

    this.pendingReheat = false;
    this.clearReheatTimers();
    this.lowVelocityTickCount = 0;
    this.pendingReheatAt = 0;

    const baseTarget = typeof this.options.alphaTarget === 'number' ? this.options.alphaTarget : 0;
    const steps = [
      { delay: 0, alpha: 0.24, target: 0.2, restart: true },
      { delay: 160, alpha: null, target: 0.12 },
      { delay: 360, alpha: null, target: 0.06 },
      { delay: 660, alpha: null, target: baseTarget }
    ];

    steps.forEach((step, index) => {
      const timerId = setTimeout(() => {
        if (!this.simulation) return;
        if (typeof step.alpha === 'number') {
          this.simulation.alpha(step.alpha);
          if (step.restart) {
            this.simulation.restart();
          }
        }
        this.simulation.alphaTarget(step.target);
        if (index === steps.length - 1) {
          this.reheatTimers = this.reheatTimers.filter(id => id !== timerId);
        }
      }, step.delay);
      this.reheatTimers.push(timerId);
    });
  }

  renderEmptyState(width, height) {
    this.interactionCircle = null;
    this.svg.selectAll("*").remove();
    
    this.svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 - 20)
      .attr("text-anchor", "middle")
      .attr("fill", "#666")
      .attr("font-size", "18px")
      .text("No data available");
      
    this.svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 + 10)
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .attr("font-size", "14px")
      .text("Try adjusting your filters");
  }

  // Drag handlers
  dragStart(event, d) {
    this.clearReheatTimers();
    this.pendingReheat = false;
    this.lowVelocityTickCount = 0;
    this.pendingReheatAt = 0;
    this.draggedDuringInteraction = false;

    d._dragStartX = d.x;
    d._dragStartY = d.y;

    if (!event.active && this.simulation) {
      this.simulation.alphaTarget(0.3).alpha(0.3).restart();
    }

    d.fx = d.x;
    d.fy = d.y;
  }

  dragMove(event, d) {
    d.fx = event.x;
    d.fy = event.y;

    const originX = Number.isFinite(d._dragStartX) ? d._dragStartX : d.x;
    const originY = Number.isFinite(d._dragStartY) ? d._dragStartY : d.y;
    const dx = event.x - originX;
    const dy = event.y - originY;
    const movementThreshold = this.options.dragMovementThreshold ?? 4;

    if (!this.draggedDuringInteraction && Math.hypot(dx, dy) > movementThreshold) {
      this.draggedDuringInteraction = true;
    }
  }

  dragEnd(event, d) {
    const moved = this.draggedDuringInteraction;
    this.draggedDuringInteraction = false;
    delete d._dragStartX;
    delete d._dragStartY;

    this.lowVelocityTickCount = 0;
    if (!event.active && this.simulation) {
      this.simulation.alpha(0.24).alphaTarget(this.options.alphaTarget || 0).restart();
      if (moved) {
        this.startRedistribution();
      } else {
        this.pendingReheat = false;
      }
    } else {
      this.pendingReheat = false;
    }
    d.vx = 0;
    d.vy = 0;
    d.fx = null;
    d.fy = null;
  }

  startRedistribution() {
    if (!this.simulation || this.isRedistributing) return;

    const now = typeof performance !== 'undefined' && typeof performance.now === 'function'
      ? performance.now()
      : Date.now();

    this.isRedistributing = true;
    this.pendingReheat = false;
    this.lowVelocityTickCount = 0;

    this.clearReheatTimers();
    this.clearRedistributionAnimations();

    const fallbackBounds = {
      cx: this.centerX ?? ((this.width || 0) / 2),
      cy: this.centerY ?? ((this.height || 0) / 2),
      R: this.boundingRadius || Math.max(1, Math.min(this.width || 0, this.height || 0) / 2)
    };
    const bounds = this.bounds || fallbackBounds;
    const centerX = bounds?.cx ?? fallbackBounds.cx;
    const centerY = bounds?.cy ?? fallbackBounds.cy;

    const { emptyRatio } = this.computeCoverageMetrics();
    const normalizedEmpty = Math.max(0, Math.min(1, emptyRatio));

    const baseCharge = this.originalChargeStrength ?? -30;
    const compressFactor = 0.55 + (1 - normalizedEmpty) * 0.15; // 0.35 - 0.60 // was 0.35, 0.25
    const expandFactor = 1.25 + normalizedEmpty * 4.75; // 1.25 - 6.0
    const maxPullStrength = 0.04 + normalizedEmpty * 0.12; // was 0.07 , 0.28
    const compressDuration = 240 + normalizedEmpty * 520;
    const expandDuration = 520 + normalizedEmpty * 320;

    const radialForce = d3.forceRadial(0, centerX, centerY).strength(0);
    this.simulation
      .force("center", null)
      .force("redistribute", radialForce);

    const chargeForce = this.simulation.force("charge");
    const collisionForce = this.simulation.force("collision");

    if (collisionForce && typeof collisionForce.iterations === 'function') {
      if (!this._originalCollisionIterations) {
        this._originalCollisionIterations = collisionForce.iterations();
      }
      collisionForce.iterations(Math.max(this._originalCollisionIterations || 1, 3));
    }

    this.simulation.alpha(Math.max(0.35, this.simulation.alpha()));
    this.simulation.alphaTarget(0.18).restart();

    const timers = [];

    const startExpansion = () => {
      const expansionTimer = d3.timer((elapsed) => {
        if (!this.isRedistributing) {
          expansionTimer.stop();
          return;
        }

        const t = Math.min(1, elapsed / expandDuration);
        const eased = d3.easeCubicOut(t);

        radialForce.strength(maxPullStrength * (1 - eased));
        if (chargeForce && typeof chargeForce.strength === 'function') {
          const startCharge = baseCharge * compressFactor;
          const targetCharge = baseCharge * expandFactor;
          chargeForce.strength(startCharge + (targetCharge - startCharge) * eased);
        }

        this.simulation.alpha(Math.max(0.16, this.simulation.alpha() * 0.98));

        if (t >= 1) {
          expansionTimer.stop();
          this.finishRedistribution();
        }
      });

      timers.push(expansionTimer);
    };

    const compressTimer = d3.timer((elapsed) => {
      if (!this.isRedistributing) {
        compressTimer.stop();
        return;
      }

      const t = Math.min(1, elapsed / compressDuration);
      const eased = d3.easeCubicIn(t);

      radialForce.strength(maxPullStrength * eased);
      if (chargeForce && typeof chargeForce.strength === 'function') {
        const startCharge = baseCharge;
        const targetCharge = baseCharge * compressFactor;
        chargeForce.strength(startCharge + (targetCharge - startCharge) * eased);
      }

      this.simulation.alpha(Math.max(0.28, this.simulation.alpha() * 0.96));

      if (t >= 1) {
        compressTimer.stop();
        startExpansion();
      }
    });

    timers.push(compressTimer);
    this.redistributionTimers = timers;

    this.pendingReheatAt = now;

    const guardDuration = compressDuration + expandDuration + 600;
    this.redistributionTimer = setTimeout(() => this.finishRedistribution(), guardDuration);
  }

  finishRedistribution() {
    if (!this.simulation || !this.isRedistributing) return;

    this.isRedistributing = false;

    this.clearRedistributionAnimations();

    const chargeForce = this.simulation.force("charge");
    if (chargeForce && typeof chargeForce.strength === 'function') {
      chargeForce.strength(this.originalChargeStrength ?? -30);
    }

    const collisionForce = this.simulation.force("collision");
    if (collisionForce && typeof collisionForce.iterations === 'function' && this._originalCollisionIterations) {
      collisionForce.iterations(this._originalCollisionIterations);
    }

    this.simulation
      .force("redistribute", null)
      .force("center", null)
      .alpha(Math.max(0.2, this.simulation.alpha()))
      .alphaTarget(this.options.alphaTarget || 0)
      .restart();

    this.lowVelocityTickCount = 0;
    this.pendingReheat = true;
    this.pendingReheatAt = typeof performance !== 'undefined' && typeof performance.now === 'function'
      ? performance.now()
      : Date.now();
  }

  // Public methods for React component to call
  updateImages(imageUpdates) {
      if (!this.nodes || this.nodes.length === 0) return;
    
      imageUpdates.forEach(update => {
        const node = this.nodes.find(n => n.id === update.nodeId);
        if (node) {
          node.image = update.imageUrl;
        
          // Update the image element - use more specific selector
          const imageEl = this.svg.select(`.img-layer`)
            .selectAll("image.node-image")
            .filter(d => d && d.id === update.nodeId);
            
          if (!imageEl.empty()) {
            imageEl
              .style("display", null)
              .attr("href", update.imageUrl);
              
            // Fade circle background
            this.svg.select(".circle-layer")
              .selectAll("circle.bubble-node")
              .filter(d => d && d.id === update.nodeId)
              .attr("fill", "none"); // Make it transparent when image loads
          }
        }
      });
    }

  setSelectionState(selectedId) {
    const hasSelection = !!selectedId;

    this.svg.selectAll("circle.bubble-node")
      .classed("bubble-dim", d => hasSelection && d && d.id !== selectedId)
      .classed("bubble-selected", d => hasSelection && d && d.id === selectedId);

    this.svg.selectAll("image.node-image")
      .classed("bubble-dim", d => hasSelection && d && d.id !== selectedId);
  }

  computeFocusTransform(node) {
    if (!node || !this.zoom) {
      return this.baseTransform || d3.zoomIdentity;
    }

    const scaleExtent = this.zoom.scaleExtent ? this.zoom.scaleExtent() : [1, CONFIG.LABEL_SETTINGS.maxZoom || this.options.focusZoom];
    const maxZoomExtent = Array.isArray(scaleExtent) ? scaleExtent[1] : (CONFIG.LABEL_SETTINGS.maxZoom || this.options.focusZoom);
    const fallbackMaxZoom = this.options.focusZoom || 2;
    const zoomRange = Array.isArray(this.options.focusZoomRange) && this.options.focusZoomRange.length >= 2
      ? this.options.focusZoomRange
      : [Math.max(1.1, fallbackMaxZoom * 0.6), fallbackMaxZoom];
    const effectiveMaxZoom = Math.min(maxZoomExtent || zoomRange[1], zoomRange[1]);
    const effectiveMinZoom = Math.max(1, Math.min(zoomRange[0], effectiveMaxZoom));

    const radii = this.nodes.map(n => n ? n.r : 0).filter(r => Number.isFinite(r) && r > 0);
    const minRadius = radii.length ? Math.min(...radii) : node.r;
    const maxRadius = radii.length ? Math.max(...radii) : node.r;
    const radiusRange = Math.max(0.0001, maxRadius - minRadius);
    const normalizedSize = Math.min(1, Math.max(0, (node.r - minRadius) / radiusRange));
    const dynamicScale = effectiveMaxZoom - (effectiveMaxZoom - effectiveMinZoom) * normalizedSize;
    const scale = Math.max(effectiveMinZoom, Math.min(effectiveMaxZoom, dynamicScale));

    const width = this.width || 0;
    const height = this.height || 0;
    const edgePadding = this.options.focusEdgePadding ?? 40;
    const scaledRadius = node.r * scale;
    const translateSlackX = width * (this.options.focusTranslateSlackRatio ?? 0.25);
    const translateSlackY = height * (this.options.focusTranslateSlackRatio ?? 0.25);

    const rawTargetX = width * this.options.focusTargetXRatio;
    const rawTargetY = height * this.options.focusTargetYRatio;
    const targetX = Math.max(scaledRadius + edgePadding, Math.min(width - scaledRadius - edgePadding, rawTargetX));
    const targetY = Math.max(scaledRadius + edgePadding, Math.min(height - scaledRadius - edgePadding, rawTargetY));

    let translateX = targetX - node.x * scale;
    let translateY = targetY - node.y * scale;

    const minTranslateX = -node.x * scale + scaledRadius + edgePadding - translateSlackX;
    const maxTranslateX = width - scaledRadius - edgePadding - node.x * scale + translateSlackX;
    translateX = Math.max(minTranslateX, Math.min(maxTranslateX, translateX));

    const minTranslateY = -node.y * scale + scaledRadius + edgePadding - translateSlackY;
    const maxTranslateY = height - scaledRadius - edgePadding - node.y * scale + translateSlackY;
    translateY = Math.max(minTranslateY, Math.min(maxTranslateY, translateY));

    return d3.zoomIdentity.translate(translateX, translateY).scale(scale);
  }

  focusOnNode(node, { animate = true } = {}) {
    if (!node || !this.zoom) return;

    const target = this.computeFocusTransform(node);
    if (animate && this.options.focusZoomDuration > 0) {
      this.animateToTransform(target);
    } else {
      this.stopActiveZoomAnimation();
      this.applyZoomTransform(target, { skipBehaviorSync: true });
      this.syncZoomBehavior(target);
    }
  }

  reapplySelection({ animate = false } = {}) {
    const id = this.selectedBubbleId;
    if (!id) {
      return;
    }

    const node = this.nodes.find(n => n && n.id === id);
    if (!node) {
      this.selectedBubbleId = null;
      this.setSelectionState(null);
      if (this.zoom) {
        this.stopActiveZoomAnimation();
        const base = this.baseTransform || d3.zoomIdentity;
        this.applyZoomTransform(base, { skipBehaviorSync: true });
        this.syncZoomBehavior(base);
      }
      return;
    }

    this.setSelectionState(id);
    this.focusOnNode(node, { animate });
  }

  selectBubble(id, options = {}) {
    this.selectedBubbleId = id;
    this.setSelectionState(id);

    const node = this.nodes.find(n => n && n.id === id);
    if (!node) {
      return;
    }

    const animate = options.animate !== false;
    this.focusOnNode(node, { animate });
  }

  clearSelection({ animate = true } = {}) {
    this.selectedBubbleId = null;
    this.setSelectionState(null);

    if (this.zoom) {
      if (animate) {
        this.animateToTransform(this.baseTransform || d3.zoomIdentity);
      } else {
        this.stopActiveZoomAnimation();
        const base = this.baseTransform || d3.zoomIdentity;
        this.applyZoomTransform(base, { skipBehaviorSync: true });
        this.syncZoomBehavior(base);
      }
    }
  }

  keepAlive() {
    if (this.simulation && this.simulation.alpha() < 0.05) {
        this.simulation.alpha(0.1).restart();
    }
 }

  destroy() {
    if (this.simulation) {
      this.simulation.stop();
    }

    this.clearReheatTimers();
    this.clearRedistributionAnimations();
    this.pendingReheat = false;
    this.isRedistributing = false;
    this.clipSel = null;
    this.imgSel = null;
    this.circleSel = null;
    this.labelSel = null;
    this.interactionCircle = null;
    this.baseTransform = d3.zoomIdentity;
    this.selectedBubbleId = null;
    this.zoom = null;
    this.gRoot = null;
    this.labelLayerGroup = null;
    this.silenceZoomEvents = false;
    this.stopActiveZoomAnimation();
    if (this.svg) {
      this.svg.interrupt("bubble-zoom");
    }
  }
}
