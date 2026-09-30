import { rotated, type Layer, type StipplePattern } from "./stipple";

// Pattern units: 1 = half the pattern's height. Inks from the print material.
const inks = { a: "#62f5c0", b: "#7383e0" };

const flute = { type: "flute", angle: 60, period: 0.16, scale: 9, bend: 0 } as const;

/** Indigo from the top-left, green mirrored from the bottom-right, sharing rib edges. */
const stripeLayers: Layer[] = [
  {
    ink: "b",
    source: { type: "radial", x: -0.9, y: -1.1, radius: 1.6, gamma: 1.6 },
    warps: [flute],
  },
  {
    ink: "a",
    source: { type: "radial", x: 0.9, y: 1.1, radius: 1.6, gamma: 1.6 },
    warps: [{ ...flute, scale: -flute.scale }],
  },
];

/** Diagonal stripes, each rib lit hard at one edge. */
export const stripePattern: StipplePattern = {
  inks,
  spacing: 0.006,
  radius: 0.0025,
  gain: 1.2,
  seed: 3,
  layers: stripeLayers,
};

const bands = { type: "flute", angle: 90, period: 0.12, scale: 9, bend: 0 } as const;

/** Horizontal stripes: green from the top-left, indigo mirrored from the bottom-right. */
const gridStripes: Layer[] = [
  {
    ink: "a",
    source: { type: "radial", x: -1.1, y: -1.1, radius: 1.9, gamma: 1.6 },
    warps: [bands],
  },
  {
    ink: "b",
    source: { type: "radial", x: 1.1, y: 1.1, radius: 1.9, gamma: 1.6 },
    warps: [{ ...bands, scale: -bands.scale }],
  },
];

/** Two stripe sets, one turned 90°, combined before sampling (brighter wins). */
export const gridPattern: StipplePattern = {
  inks,
  spacing: 0.006,
  radius: 0.0025,
  gain: 1,
  seed: 7,
  blend: "max",
  layers: [...gridStripes, ...rotated(gridStripes, 90)],
};
