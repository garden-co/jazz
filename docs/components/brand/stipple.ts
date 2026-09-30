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

/** A point on its way from the page to the source, and how much it is dimmed. */
type Probe = { x: number; y: number; weight: number };

/**
 * Compiles a warp into a function that moves a probe towards the source.
 * Everything that depends only on the warp's settings is worked out here,
 * once, rather than per point.
 */
function compileWarp(w: Warp): (p: Probe) => void {
  switch (w.type) {
    case "flute": {
      const cos = Math.cos(rad(w.angle));
      const sin = Math.sin(rad(w.angle));
      const phase = w.phase ?? 0;
      if ((w.projection ?? "orthographic") === "orthographic") {
        const falloff = w.falloff ?? 1;
        const mirror = w.mirror ?? false;
        return (p) => {
          const u = (p.x * cos + p.y * sin) / w.period + phase;
          const local = u - Math.floor(u) - 0.5;
          // How far across the rib from its lit edge, 0 to 1.
          const along = mirror ? 0.5 - local : local + 0.5;
          const lit = 1 - along;
          p.weight *= falloff === 1 ? lit : Math.pow(lit, falloff);
        };
      }
      const scale = w.scale ?? 1;
      const shift = w.shift ?? 0;
      const bend = w.bend ?? 0;
      return (p) => {
        // Across the ribs (t) and along them (s).
        const t = p.x * cos + p.y * sin;
        const s = -p.x * sin + p.y * cos;
        const u = t / w.period + phase;
        const rib = Math.floor(u);
        const local = u - rib - 0.5;
        const lens = 1 + bend * 4 * local * local;
        const t2 = (rib + 0.5 + local * scale * lens + shift - phase) * w.period;
        p.x = t2 * cos - s * sin;
        p.y = t2 * sin + s * cos;
      };
    }
    case "mirror": {
      const c = Math.cos(rad(2 * w.angle));
      const s = Math.sin(rad(2 * w.angle));
      return (p) => {
        const x = p.x;
        p.x = x * c + p.y * s;
        p.y = x * s - p.y * c;
      };
    }
    case "rotate": {
      const c = Math.cos(rad(-w.angle));
      const s = Math.sin(rad(-w.angle));
      return (p) => {
        const x = p.x;
        p.x = x * c - p.y * s;
        p.y = x * s + p.y * c;
      };
    }
    case "translate":
      return (p) => {
        p.x -= w.x;
        p.y -= w.y;
      };
  }
}

function compileSource(src: Source): (x: number, y: number) => number {
  const gamma = src.gamma ?? 1;
  const shape = (v: number) => (v <= 0 ? 0 : v >= 1 ? 1 : gamma === 1 ? v : Math.pow(v, gamma));
  if (src.type === "radial") {
    const cx = src.x ?? 0;
    const cy = src.y ?? 0;
    const r2 = src.radius * src.radius;
    return (x, y) => {
      const d2 = (x - cx) * (x - cx) + (y - cy) * (y - cy);
      return d2 >= r2 ? 0 : shape(1 - Math.sqrt(d2 / r2));
    };
  }
  const cos = Math.cos(rad(src.angle));
  const sin = Math.sin(rad(src.angle));
  return (x, y) => shape((x * cos + y * sin - src.from) / (src.to - src.from));
}

/** Compiles one layer into a density function of a page point. */
export function compileLayer(layer: Layer): (x: number, y: number) => number {
  const warps = (layer.warps ?? []).map(compileWarp);
  const read = compileSource(layer.source);
  const gain = layer.gain ?? 1;
  const probe: Probe = { x: 0, y: 0, weight: 1 };
  return (x, y) => {
    probe.x = x;
    probe.y = y;
    probe.weight = 1;
    for (const warp of warps) warp(probe);
    if (probe.weight <= 0) return 0;
    return clamp01(read(probe.x, probe.y) * probe.weight * gain);
  };
}

/** Density of one layer at a page point. */
export function layerAt(layer: Layer, x: number, y: number) {
  return compileLayer(layer)(x, y);
}

export type Blend = "screen" | "max" | "multiply";

/** Compiles the layers of one ink into its combined density; 0 if it has none. */
function compileInk(layers: Layer[], ink: Ink, blend: Blend): (x: number, y: number) => number {
  const parts = layers.filter((layer) => layer.ink === ink).map(compileLayer);
  if (parts.length === 0) return () => 0;
  return (x, y) => {
    let d = parts[0](x, y);
    for (let i = 1; i < parts.length; i++) {
      const v = parts[i](x, y);
      d = blend === "screen" ? 1 - (1 - d) * (1 - v) : blend === "max" ? Math.max(d, v) : d * v;
    }
    return d;
  };
}

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
  return { a: compileInk(layers, "a", blend)(x, y), b: compileInk(layers, "b", blend)(x, y) };
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
  const blend = pattern.blend ?? "screen";
  const inkA = compileInk(layers, "a", blend);
  const inkB = compileInk(layers, "b", blend);
  const gain = pattern.gain ?? 1;
  const passes: (Ink | "both")[] = pattern.sampling === "shared" ? ["both"] : ["a", "b"];
  const each = pattern.points === "blue-noise" ? blueNoisePoints : gridPoints;
  for (const pass of passes) {
    each(pattern, bounds, pass === "b" ? 1 : 0, (x, y, rank) => {
      // Each pass reads only the inks it can place.
      let a = pass === "b" ? 0 : clamp01(inkA(x, y) * gain);
      let b = pass === "a" ? 0 : clamp01(inkB(x, y) * gain);
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
