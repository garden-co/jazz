import type { Flute, Layer, StipplePattern } from "./stipple";

// Pattern units: 1 = half the pattern's height. Inks from the print material.
const inks = { a: "#62f5c0", b: "#7383e0" };

/**
 * Each ink is a radial gradient centred near the middle (the two slightly
 * apart) behind the same fluted glass, seen straight on: green's ribs are lit
 * hard at their leading edge and fade across, indigo's are mirrored (lit hard
 * at the trailing edge), so the two inks' sharp edges meet on the shared rib
 * lines everywhere.
 */
function stripes(flute: Flute, radius: number, offset: number): Layer[] {
  return [
    {
      ink: "a",
      source: { type: "radial", x: -offset, y: -offset, radius, gamma: 1.4 },
      warps: [flute],
    },
    {
      ink: "b",
      source: { type: "radial", x: offset, y: offset, radius, gamma: 1.4 },
      warps: [{ ...flute, mirror: !flute.mirror }],
    },
  ];
}

/** Diagonal stripes. */
export const stripePattern: StipplePattern = {
  inks,
  spacing: 0.006,
  radius: 0.0025,
  gain: 1.4,
  seed: 3,
  points: "blue-noise",
  layers: stripes({ type: "flute", angle: 60, period: 0.16, falloff: 2.5 }, 1.4, 0.12),
};

/**
 * Stripes across and down, each with its own fluting width, combined per ink
 * before sampling. "screen" keeps both sets of lines visible where they
 * cross; "multiply" would keep only the crossings (a checkerboard).
 */
export const gridPattern: StipplePattern = {
  inks,
  spacing: 0.006,
  radius: 0.0025,
  gain: 1.2,
  seed: 7,
  points: "blue-noise",
  blend: "screen",
  layers: [
    ...stripes({ type: "flute", angle: 0, period: 0.12, falloff: 4 }, 1.4, 0.12),
    ...stripes({ type: "flute", angle: 90, period: 0.12, falloff: 4 }, 1.4, 0.12),
  ],
};
