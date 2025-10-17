function sum(array) {
  return array.reduce((acc, value) => acc + (Number.isFinite(value) ? value : 0), 0);
}

function extractShares(items) {
  if (!Array.isArray(items)) {
    return [];
  }

  const pctValues = items.map((item) => {
    const raw = Number.isFinite(item && item.value_pct) ? item.value_pct : 0;
    return raw > 0 ? raw : 0;
  });

  const maxPct = pctValues.reduce((acc, value) => (value > acc ? value : acc), 0);
  const treatAsPercentUnits = maxPct > 1;

  const shares = pctValues.map((value) => {
    if (!(value > 0)) {
      return 0;
    }
    return treatAsPercentUnits ? value / 100 : value;
  });

  return shares;
}

function computeProportionalRadii(items, targetArea) {
  if (!Array.isArray(items) || !items.length) {
    return [];
  }

  const shares = extractShares(items);
  let valuesForScaling = shares;
  let sumForScaling = sum(shares);

  if (!(sumForScaling > 0)) {
    valuesForScaling = items.map((item) => {
      const ms = Number.isFinite(item && item.value_ms) ? item.value_ms : 0;
      return ms > 0 ? ms : 0;
    });
    sumForScaling = sum(valuesForScaling);
  }

  if (!(sumForScaling > 0) || !(targetArea > 0)) {
    return valuesForScaling.map(() => 0);
  }

  const areaScale = targetArea / sumForScaling;

  return valuesForScaling.map((value) => {
    const area = Math.max(0, value * areaScale);
    return Math.sqrt(area / Math.PI);
  });
}

function shareRatioToRadiusRatio(shareA, shareB) {
  const maxShare = Math.max(shareA || 0, shareB || 0);
  const treatAsPercentUnits = maxShare > 1;

  const normalizedA = treatAsPercentUnits ? (shareA / 100) : shareA;
  const normalizedB = treatAsPercentUnits ? (shareB / 100) : shareB;

  if (!(normalizedA > 0) || !(normalizedB > 0)) {
    return 0;
  }

  return Math.sqrt(normalizedA / normalizedB);
}

module.exports = {
  computeProportionalRadii,
  shareRatioToRadiusRatio,
};

