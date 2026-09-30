import type { StipplePattern } from "./stipple";

// Pattern units: 1 = half the pattern's height. Inks from the print material.
const inks = { a: "#419373", b: "#465986" };

/** Concentric bands around a checkered grid, green top-left, indigo bottom-right. */
export const gridPattern: StipplePattern = {
  inks,
  spacing: 0.005,
  radius: 0.0021,
  gain: 1.5,
  jitter: 1,
  seed: 7,
  fields: [
    // Rings: concentric rectangles outside the grid, open at the corners.
    {
      layers: [
        {
          channel: "ab",
          coordinate: { type: "box", aspect: 1.1 },
          profile: { type: "pulse", period: 0.1, phase: 0.05, duty: 0.5, soft: 0.3, fade: 0.8 },
        },
        {
          channel: "ab",
          coordinate: { type: "box", aspect: 1.1 },
          profile: { type: "ramp", from: 0.42, to: 0.5 },
        },
        {
          channel: "ab",
          coordinate: { type: "diagonal", aspect: 1.1 },
          profile: { type: "ramp", from: 0.02, to: 0.12 },
        },
        {
          channel: "ab",
          coordinate: { type: "box", aspect: 1.1 },
          profile: { type: "ramp", from: 1.05, to: 0.75 },
        },
        // Green top-left, indigo bottom-right, blending across the middle.
        {
          channel: "a",
          coordinate: { type: "linear", angle: 45 },
          profile: { type: "ramp", from: 0.35, to: -0.35 },
        },
        {
          channel: "b",
          coordinate: { type: "linear", angle: 45 },
          profile: { type: "ramp", from: -0.35, to: 0.35 },
        },
      ],
    },
    // Grid, green: rows lit hard at the top edge, fading downward.
    {
      inks: "a",
      layers: [
        {
          channel: "a",
          coordinate: { type: "linear", angle: 90 },
          profile: { type: "pulse", period: 0.1, duty: 0.8, soft: 0.1, fade: 0.9 },
        },
        {
          channel: "a",
          coordinate: { type: "linear", angle: 0 },
          profile: { type: "pulse", period: 0.13, duty: 0.85, soft: 0.2, fade: -0.5 },
        },
        {
          channel: "a",
          coordinate: { type: "box", aspect: 1.1 },
          profile: { type: "ramp", from: 0.5, to: 0.42 },
        },
        {
          channel: "a",
          coordinate: { type: "linear", angle: 60 },
          profile: { type: "ramp", from: 0.5, to: -0.3 },
        },
      ],
    },
    // Grid, indigo: rows lit hard at the bottom edge, fading upward.
    {
      inks: "b",
      layers: [
        {
          channel: "b",
          coordinate: { type: "linear", angle: 90 },
          profile: { type: "pulse", period: 0.1, phase: 0.1, duty: 0.8, soft: 0.1, fade: -0.9 },
        },
        {
          channel: "b",
          coordinate: { type: "linear", angle: 0 },
          profile: { type: "pulse", period: 0.13, phase: 0.5, duty: 0.85, soft: 0.2, fade: 0.5 },
        },
        {
          channel: "b",
          coordinate: { type: "box", aspect: 1.1 },
          profile: { type: "ramp", from: 0.5, to: 0.42 },
        },
        {
          channel: "b",
          coordinate: { type: "linear", angle: 60 },
          profile: { type: "ramp", from: -0.5, to: 0.3 },
        },
      ],
    },
  ],
};

/** Diagonal stripes, indigo above, green below, each band lit hard at one edge. */
export const stripePattern: StipplePattern = {
  inks,
  spacing: 0.005,
  radius: 0.0021,
  gain: 1.5,
  jitter: 1,
  seed: 3,
  fields: [
    {
      layers: [
        {
          channel: "ab",
          coordinate: { type: "linear", angle: 60 },
          profile: { type: "pulse", period: 0.16, duty: 0.75, soft: 0.1, fade: 0.85 },
        },
        {
          channel: "a",
          coordinate: { type: "linear", angle: 90 },
          profile: { type: "ramp", from: -0.6, to: 0.4 },
        },
        {
          channel: "b",
          coordinate: { type: "linear", angle: 90 },
          profile: { type: "ramp", from: 0.6, to: -0.2 },
        },
      ],
    },
  ],
};
