import assert from "node:assert/strict";
import bubbleMath from "../spotify-analytics-dashboard/src/utils/bubbleMath.js";

const { computeProportionalRadii, shareRatioToRadiusRatio } = bubbleMath;

function generateItems(iteration) {
  const usePercentUnits = iteration % 2 === 0;
  const count = 3 + (iteration % 7);
  const items = [];

  for (let idx = 0; idx < count; idx += 1) {
    const seed = Math.abs(Math.sin((iteration + 1) * (idx + 1) * 13.37));
    const share = usePercentUnits
      ? Number((0.25 + seed * 35).toFixed(4)) // percentage units (> 1)
      : Number((0.005 + seed * 0.45).toFixed(6)); // fractional units (< 1)

    items.push({
      id: `item-${iteration}-${idx}`,
      label: `Item ${iteration}-${idx}`,
      value_pct: share,
      value_ms: Math.round(share * 1_000_000),
    });
  }

  return items;
}

const targetArea = Math.PI * 12000;
const tolerance = 1e-6;
let comparisonsChecked = 0;

for (let iteration = 0; iteration < 20; iteration += 1) {
  const items = generateItems(iteration);
  const radii = computeProportionalRadii(items, targetArea);

  assert.equal(radii.length, items.length, "expected radii for every item");

  const positiveIndices = items
    .map((item, index) => ({ index, share: item.value_pct }))
    .filter(({ share }) => Number(share) > 0);

  assert.ok(positiveIndices.length >= 2, "need at least two positive-share items");

  for (let i = 0; i < positiveIndices.length; i += 1) {
    for (let j = i + 1; j < positiveIndices.length; j += 1) {
      const first = positiveIndices[i];
      const second = positiveIndices[j];

      const radiusRatio = radii[first.index] / radii[second.index];
      const expectedRatio = shareRatioToRadiusRatio(first.share, second.share);

      assert.ok(Number.isFinite(radiusRatio) && radiusRatio > 0, "radius ratio must be finite and positive");
      assert.ok(Number.isFinite(expectedRatio) && expectedRatio > 0, "expected ratio must be finite and positive");
      assert.ok(
        Math.abs(radiusRatio - expectedRatio) < tolerance,
        `radius ratio ${radiusRatio} should match sqrt share ratio ${expectedRatio} (iteration ${iteration}, pair ${i}-${j})`
      );
      comparisonsChecked += 1;
    }
  }
}

assert.ok(comparisonsChecked >= 20, `expected at least 20 comparisons, got ${comparisonsChecked}`);

console.log(`Bubble scaling test passed for ${comparisonsChecked} share comparisons across 20 datasets.`);
