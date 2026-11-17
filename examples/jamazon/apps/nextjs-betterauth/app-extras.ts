// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema is schema.ts (for `generate` only); its types are in jazz.generated.ts.

/** Token hues used by the generated product illustrations. */
export const HUES = [
  "gray",
  "blue",
  "cyan",
  "green",
  "orange",
  "pink",
  "purple",
  "red",
  "teal",
  "yellow",
] as const;
/** The illustration drawn for a product; see src/components/ProductArt.tsx. */
export const ART = [
  "guitar",
  "bass",
  "keys",
  "synth",
  "drum",
  "cymbal",
  "mic",
  "headphones",
  "amp",
  "pedal",
  "cable",
  "strings",
  "picks",
] as const;
export type Hue = (typeof HUES)[number];
export type Art = (typeof ART)[number];
