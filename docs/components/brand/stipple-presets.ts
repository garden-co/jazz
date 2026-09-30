import type { Flute, Layer, StipplePattern } from "./stipple";

// Pattern units: 1 = half the pattern's height. Inks from the print material.
const inks = { a: "#62f5c0", b: "#7383e0" };

/**
 * Each ink is a radial gradient centred near the middle (the two slightly
 * apart) behind the same fluted glass. Each rib shows a stretched slice of the
 * gradient, so away from the centre green runs sharp to fuzzy across each rib.
 * Indigo's ribs are mirrored (negated scale), so it runs fuzzy to sharp, and
 * the two inks' sharp edges meet on the shared rib lines.
 */
function stripes(flute: Flute, radius: number, offset: number): Layer[] {
  return [
    {
      ink: "a",
      source: { type: "radial", x: -offset, y: -offset, radius, gamma: 2.2 },
      warps: [flute],
    },
    {
      ink: "b",
      source: { type: "radial", x: offset, y: offset, radius, gamma: 2.2 },
      warps: [{ ...flute, scale: -(flute.scale ?? 1) }],
    },
  ];
}

/** Diagonal stripes. */
export const stripePattern: StipplePattern = {
  inks,
  spacing: 0.006,
  radius: 0.0025,
  gain: 1.2,
  seed: 3,
  layers: stripes({ type: "flute", angle: 60, period: 0.16, scale: 10 }, 1.4, 0.12),
};

/** Horizontal stripes plus a copy turned 90°, combined before sampling. */
export const gridPattern: StipplePattern = {
  inks,
  spacing: 0.006,
  radius: 0.0025,
  gain: 1,
  seed: 7,
  blend: "max",
  copies: [90],
  layers: stripes({ type: "flute", angle: 90, period: 0.12, scale: 10 }, 1.3, 0.12),
};
