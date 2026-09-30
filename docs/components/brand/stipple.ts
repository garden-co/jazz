// Duotone stippling: the dotted stripe and grid patterns from Jazz print
// material, generated instead of shipped as images.
//
// Each ink layer is a filter chain read from the output back to a source:
// a point on the page is pushed through the layer's warps in order (fluted
// glass, mirror, rotate, translate) and the source gradient is read where it
// lands. Fluted glass cuts the plane into ribs and shows a stretched slice of
// the gradient in each, which is where the sharp edges come from. Stripes
// are a radial gradient behind fluted glass per ink, the second ink mirrored
// so its sharp edges meet the first's; a grid is the stripes again, turned
// 90°.
//
// The resulting densities are sampled into dots on jittered grids, one grid
// per ink (dots of different inks may overlap, as in print) or one shared
// grid (at most one dot per cell).

export type Ink = "a" | "b";

export type Source =
  /** 1 at the centre falling to 0 at `radius`, shaped by `gamma`. */
  | { type: "radial"; x?: number; y?: number; radius: number; gamma?: number }
  /** 0 at `from` rising to 1 at `to`, along `angle` (0° right, 90° down). */
  | { type: "linear"; angle: number; from: number; to: number; gamma?: number };

export type Warp =
  /**
   * Fluted glass with ribs perpendicular to `angle`, `period` apart. Each rib
   * shows the plane behind it scaled by `scale` around the rib's centre
   * (negative flips it), shifted by `shift` periods; `bend` curves the scale
   * towards the rib edges like a real lens.
   */
  | {
      type: "flute";
      angle: number;
      period: number;
      scale?: number;
      shift?: number;
      bend?: number;
      phase?: number;
    }
  /** Mirrors across the line through the origin at `angle`. */
  | { type: "mirror"; angle: number }
  | { type: "rotate"; angle: number }
  | { type: "translate"; x: number; y: number };

export type Layer = {
  ink: Ink;
  source: Source;
  /** Applied in order, from the page towards the source. */
  warps?: Warp[];
  /** Multiplies this layer's density. */
  gain?: number;
};

export type StipplePattern = {
  layers: Layer[];
  inks: { a: string; b: string };
  /** Distance between sample cells, in pattern units. */
  spacing: number;
  /** Dot radius, in pattern units. */
  radius: number;
  /** 0 = dots on a regular grid, 1 = anywhere in their cell. */
  jitter?: number;
  /** Scales both densities before sampling. */
  gain?: number;
  /** How layers of the same ink combine; defaults to "screen". */
  blend?: Blend;
  /** "independent" samples each ink on its own grid; "shared" allows one dot per cell. */
  sampling?: "independent" | "shared";
  seed?: number;
};

export type Densities = { a: number; b: number };

const clamp01 = (v: number) => (v < 0 ? 0 : v > 1 ? 1 : v);
const rad = (deg: number) => (deg * Math.PI) / 180;

function warp(w: Warp, x: number, y: number): [number, number] {
  switch (w.type) {
    case "flute": {
      const cos = Math.cos(rad(w.angle));
      const sin = Math.sin(rad(w.angle));
      // Across the ribs (t) and along them (s).
      const t = x * cos + y * sin;
      const s = -x * sin + y * cos;
      const u = t / w.period + (w.phase ?? 0);
      const rib = Math.floor(u);
      const local = u - rib - 0.5;
      const bend = 1 + (w.bend ?? 0) * 4 * local * local;
      const t2 =
        (rib + 0.5 + local * (w.scale ?? 1) * bend + (w.shift ?? 0) - (w.phase ?? 0)) * w.period;
      return [t2 * cos - s * sin, t2 * sin + s * cos];
    }
    case "mirror": {
      const c = Math.cos(rad(2 * w.angle));
      const s = Math.sin(rad(2 * w.angle));
      return [x * c + y * s, x * s - y * c];
    }
    case "rotate": {
      const c = Math.cos(rad(-w.angle));
      const s = Math.sin(rad(-w.angle));
      return [x * c - y * s, x * s + y * c];
    }
    case "translate":
      return [x - w.x, y - w.y];
  }
}

function source(src: Source, x: number, y: number) {
  let v: number;
  if (src.type === "radial") {
    v = 1 - Math.hypot(x - (src.x ?? 0), y - (src.y ?? 0)) / src.radius;
  } else {
    const t = x * Math.cos(rad(src.angle)) + y * Math.sin(rad(src.angle));
    v = (t - src.from) / (src.to - src.from);
  }
  v = clamp01(v);
  return src.gamma ? Math.pow(v, src.gamma) : v;
}

/** Density of one layer at a page point. */
export function layerAt(layer: Layer, x: number, y: number) {
  let px = x;
  let py = y;
  for (const w of layer.warps ?? []) [px, py] = warp(w, px, py);
  return clamp01(source(layer.source, px, py) * (layer.gain ?? 1));
}

export type Blend = "screen" | "max" | "multiply";

/**
 * Ink densities (0–1 each) at a page point in pattern units, centre at the
 * origin. Layers of the same ink combine by `blend`: "screen" overlays them
 * like light, "max" keeps the brighter, "multiply" keeps only where all are lit.
 */
export function densitiesAt(
  layers: Layer[],
  x: number,
  y: number,
  blend: Blend = "screen",
): Densities {
  const d = { a: -1, b: -1 };
  for (const layer of layers) {
    const v = layerAt(layer, x, y);
    const prev = d[layer.ink];
    d[layer.ink] =
      prev < 0
        ? v
        : blend === "screen"
          ? 1 - (1 - prev) * (1 - v)
          : blend === "max"
            ? Math.max(prev, v)
            : prev * v;
  }
  return { a: Math.max(d.a, 0), b: Math.max(d.b, 0) };
}

/** Stateless hash of a grid cell to three uniform numbers in [0, 1). */
function cellRandom(seed: number, i: number, j: number, out: number[]) {
  let h =
    Math.imul(seed ^ 0x9e3779b9, 0x85ebca6b) ^ Math.imul(i, 0xc2b2ae35) ^ Math.imul(j, 0x27d4eb2f);
  for (let k = 0; k < 3; k++) {
    h = Math.imul(h ^ (h >>> 16), 0x7feb352d);
    h = Math.imul(h ^ (h >>> 15), 0x846ca68b);
    h ^= h >>> 16;
    out[k] = (h >>> 0) / 4294967296;
    h += 0x6d2b79f5;
  }
}

export type Dot = { x: number; y: number; ink: Ink };

/**
 * Samples the pattern over a rectangle (pattern units, centre at the origin)
 * and calls `emit` per dot. Each cell's randomness is a hash of its index,
 * so the same seed gives the same dots wherever the rectangle falls.
 */
export function stipple(
  pattern: StipplePattern,
  bounds: { left: number; top: number; right: number; bottom: number },
  emit: (dot: Dot) => void,
) {
  const { spacing, layers } = pattern;
  const jitter = pattern.jitter ?? 1;
  const gain = pattern.gain ?? 1;
  const seed = pattern.seed ?? 1;
  const shared = pattern.sampling === "shared";
  const i0 = Math.floor(bounds.left / spacing);
  const i1 = Math.ceil(bounds.right / spacing);
  const j0 = Math.floor(bounds.top / spacing);
  const j1 = Math.ceil(bounds.bottom / spacing);
  const r = [0, 0, 0];
  const passes: (Ink | "both")[] = shared ? ["both"] : ["a", "b"];
  for (const pass of passes) {
    const passSeed = pass === "b" ? seed + 0x51ed27 : seed;
    for (let j = j0; j <= j1; j++) {
      for (let i = i0; i <= i1; i++) {
        cellRandom(passSeed, i, j, r);
        const x = (i + 0.5 + (r[0] - 0.5) * jitter) * spacing;
        const y = (j + 0.5 + (r[1] - 0.5) * jitter) * spacing;
        const d = densitiesAt(layers, x, y, pattern.blend);
        let a = pass === "b" ? 0 : clamp01(d.a * gain);
        let b = pass === "a" ? 0 : clamp01(d.b * gain);
        const sum = a + b;
        if (sum > 1) {
          a /= sum;
          b /= sum;
        }
        if (r[2] < a) emit({ x, y, ink: "a" });
        else if (r[2] < a + b) emit({ x, y, ink: "b" });
      }
    }
  }
}

/** The same layers turned by `angle`, for grids made of two stripe sets. */
export function rotated(layers: Layer[], angle: number): Layer[] {
  return layers.map((layer) => ({
    ...layer,
    warps: [{ type: "rotate", angle }, ...(layer.warps ?? [])],
  }));
}
