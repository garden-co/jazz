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
// The resulting densities are sampled into dots, either on jittered grids or
// on a tiled blue-noise point set whose points are ranked so any prefix is
// evenly spread: a point becomes a dot where its rank is below the density,
// which keeps dots evenly spaced at every density and edges crisp. Each ink
// gets its own points (dots of different inks may overlap, as in print), or
// the inks share one set (at most one dot per point).

export type Ink = "a" | "b";

export type Source =
  /** 1 at the centre falling to 0 at `radius`, shaped by `gamma`. */
  | { type: "radial"; x?: number; y?: number; radius: number; gamma?: number }
  /** 0 at `from` rising to 1 at `to`, along `angle` (0° right, 90° down). */
  | { type: "linear"; angle: number; from: number; to: number; gamma?: number };

/**
 * Fluted glass with ribs perpendicular to `angle`, `period` apart.
 *
 * "orthographic" (the default) looks straight through each rib: the plane is
 * unchanged, but every rib is lit hard at its leading edge and fades towards
 * its trailing edge (`falloff` shapes the fade, `mirror` swaps the edges), the
 * same way everywhere on the page.
 *
 * "perspective" shows the plane behind each rib scaled by `scale` around the
 * rib's centre (negative flips it), shifted by `shift` periods; `bend` curves
 * the scale towards the rib edges like a real lens. Which edge ends up sharp
 * then depends on which way the gradient runs behind the rib.
 */
export type Flute = {
  type: "flute";
  angle: number;
  period: number;
  projection?: "orthographic" | "perspective";
  falloff?: number;
  mirror?: boolean;
  scale?: number;
  shift?: number;
  bend?: number;
  phase?: number;
};

export type Warp =
  | Flute
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
  /** For grid points: 0 = a regular grid, 1 = anywhere in their cell. */
  jitter?: number;
  /** Scales both densities before sampling. */
  gain?: number;
  /**
   * Extra copies of all layers, turned by these angles and combined with the
   * originals before sampling (a grid is stripes plus a copy at 90°).
   */
  copies?: number[];
  /** How layers of the same ink combine; defaults to "screen". */
  blend?: Blend;
  /** "independent" samples each ink on its own points; "shared" allows one dot per point. */
  sampling?: "independent" | "shared";
  /**
   * Where dots can go: "grid" is one candidate per cell, moved by `jitter`;
   * "blue-noise" is an evenly spread random point set with no grid to it.
   */
  points?: "grid" | "blue-noise";
  seed?: number;
};

export type Densities = { a: number; b: number };

const clamp01 = (v: number) => (v < 0 ? 0 : v > 1 ? 1 : v);
const rad = (deg: number) => (deg * Math.PI) / 180;

/** Maps a page point towards the source; the third value dims the reading. */
function warp(w: Warp, x: number, y: number): [number, number, number] {
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
      if ((w.projection ?? "orthographic") === "orthographic") {
        const along = w.mirror ? 0.5 - local : local + 0.5;
        return [x, y, Math.pow(1 - along, w.falloff ?? 1)];
      }
      const bend = 1 + (w.bend ?? 0) * 4 * local * local;
      const t2 =
        (rib + 0.5 + local * (w.scale ?? 1) * bend + (w.shift ?? 0) - (w.phase ?? 0)) * w.period;
      return [t2 * cos - s * sin, t2 * sin + s * cos, 1];
    }
    case "mirror": {
      const c = Math.cos(rad(2 * w.angle));
      const s = Math.sin(rad(2 * w.angle));
      return [x * c + y * s, x * s - y * c, 1];
    }
    case "rotate": {
      const c = Math.cos(rad(-w.angle));
      const s = Math.sin(rad(-w.angle));
      return [x * c - y * s, x * s + y * c, 1];
    }
    case "translate":
      return [x - w.x, y - w.y, 1];
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
  let weight = 1;
  for (const w of layer.warps ?? []) {
    const [nx, ny, dim] = warp(w, px, py);
    px = nx;
    py = ny;
    weight *= dim;
  }
  return clamp01(source(layer.source, px, py) * weight * (layer.gain ?? 1));
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
 * and calls `emit` per dot. Candidate points depend only on the seed, never
 * on the rectangle, so the same seed gives the same dots wherever it falls.
 */
export function stipple(
  pattern: StipplePattern,
  bounds: { left: number; top: number; right: number; bottom: number },
  emit: (dot: Dot) => void,
) {
  const layers = expandCopies(pattern);
  const gain = pattern.gain ?? 1;
  const passes: (Ink | "both")[] = pattern.sampling === "shared" ? ["both"] : ["a", "b"];
  const each = pattern.points === "blue-noise" ? blueNoisePoints : gridPoints;
  for (const pass of passes) {
    each(pattern, bounds, pass === "b" ? 1 : 0, (x, y, rank) => {
      const d = densitiesAt(layers, x, y, pattern.blend);
      let a = pass === "b" ? 0 : clamp01(d.a * gain);
      let b = pass === "a" ? 0 : clamp01(d.b * gain);
      const sum = a + b;
      if (sum > 1) {
        a /= sum;
        b /= sum;
      }
      if (rank < a) emit({ x, y, ink: "a" });
      else if (rank < a + b) emit({ x, y, ink: "b" });
    });
  }
}

type Bounds = { left: number; top: number; right: number; bottom: number };
type Visit = (x: number, y: number, rank: number) => void;

/** One jittered candidate per grid cell, with a random rank. */
function gridPoints(pattern: StipplePattern, bounds: Bounds, pass: number, visit: Visit) {
  const { spacing } = pattern;
  const jitter = pattern.jitter ?? 1;
  const seed = (pattern.seed ?? 1) + pass * 0x51ed27;
  const r = [0, 0, 0];
  const j1 = Math.ceil(bounds.bottom / spacing);
  const i1 = Math.ceil(bounds.right / spacing);
  for (let j = Math.floor(bounds.top / spacing); j <= j1; j++) {
    for (let i = Math.floor(bounds.left / spacing); i <= i1; i++) {
      cellRandom(seed, i, j, r);
      visit(
        (i + 0.5 + (r[0] - 0.5) * jitter) * spacing,
        (j + 0.5 + (r[1] - 0.5) * jitter) * spacing,
        r[2],
      );
    }
  }
}

/** Side of the repeating blue-noise tile, in cells (one point per cell). */
const TILE = 64;
const tiles = new Map<number, Float64Array>();

/**
 * A tileable set of TILE² points in [0, TILE)², ordered so every prefix is
 * evenly spread (Mitchell's best candidate on a torus): point k is the
 * candidate farthest from points 0..k-1. Stored as x, y pairs in rank order.
 */
export function blueNoiseTile(seed: number): Float64Array {
  const cached = tiles.get(seed);
  if (cached) return cached;
  const count = TILE * TILE;
  const out = new Float64Array(count * 2);
  // Buckets of one cell each, for nearest-point queries.
  const buckets: number[][] = Array.from({ length: count }, () => []);
  const r = [0, 0, 0];
  const nearest = (x: number, y: number) => {
    const cx = Math.floor(x);
    const cy = Math.floor(y);
    let best = Infinity;
    for (let ring = 0; ring <= TILE / 2; ring++) {
      if (best < (ring - 1) * (ring - 1)) break;
      for (let dy = -ring; dy <= ring; dy++) {
        for (let dx = -ring; dx <= ring; dx++) {
          if (Math.max(Math.abs(dx), Math.abs(dy)) !== ring) continue;
          const bucket = buckets[((cy + dy + TILE) % TILE) * TILE + ((cx + dx + TILE) % TILE)];
          for (const k of bucket) {
            let ex = Math.abs(out[2 * k] - x);
            let ey = Math.abs(out[2 * k + 1] - y);
            if (ex > TILE / 2) ex = TILE - ex;
            if (ey > TILE / 2) ey = TILE - ey;
            best = Math.min(best, ex * ex + ey * ey);
          }
        }
      }
    }
    return best;
  };
  for (let k = 0; k < count; k++) {
    let bx = 0;
    let by = 0;
    let bestDistance = -1;
    for (let c = 0; c < 32; c++) {
      cellRandom(seed, k, c, r);
      const x = r[0] * TILE;
      const y = r[1] * TILE;
      const distance = k === 0 ? 0 : nearest(x, y);
      if (distance > bestDistance) {
        bestDistance = distance;
        bx = x;
        by = y;
      }
    }
    out[2 * k] = bx;
    out[2 * k + 1] = by;
    buckets[Math.floor(by) * TILE + Math.floor(bx)].push(k);
  }
  tiles.set(seed, out);
  return out;
}

/** Blue-noise candidates repeated over the plane; rank is the point's order in its tile. */
function blueNoisePoints(pattern: StipplePattern, bounds: Bounds, pass: number, visit: Visit) {
  const { spacing } = pattern;
  const tile = blueNoiseTile(pattern.seed ?? 1);
  const count = tile.length / 2;
  const size = TILE * spacing;
  // The second ink reads the same tile shifted, so its dots fall elsewhere.
  const ox = pass * 0.37 * size;
  const oy = pass * 0.61 * size;
  const ty1 = Math.floor((bounds.bottom - oy) / size);
  const tx1 = Math.floor((bounds.right - ox) / size);
  for (let ty = Math.floor((bounds.top - oy) / size); ty <= ty1; ty++) {
    for (let tx = Math.floor((bounds.left - ox) / size); tx <= tx1; tx++) {
      for (let k = 0; k < count; k++) {
        const x = ox + tx * size + tile[2 * k] * spacing;
        const y = oy + ty * size + tile[2 * k + 1] * spacing;
        if (x < bounds.left || x > bounds.right || y < bounds.top || y > bounds.bottom) continue;
        visit(x, y, (k + 0.5) / count);
      }
    }
  }
}

/** The same layers turned by `angle`. */
export function rotated(layers: Layer[], angle: number): Layer[] {
  return layers.map((layer) => ({
    ...layer,
    warps: [{ type: "rotate", angle }, ...(layer.warps ?? [])],
  }));
}

/** The pattern's layers plus its rotated copies. */
export function expandCopies(pattern: Pick<StipplePattern, "layers" | "copies">): Layer[] {
  return [
    ...pattern.layers,
    ...(pattern.copies ?? []).flatMap((angle) => rotated(pattern.layers, angle)),
  ];
}
