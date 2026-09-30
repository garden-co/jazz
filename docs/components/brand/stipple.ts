// Duotone stippling: the dotted grid and stripe patterns from Jazz print
// material, generated instead of shipped as images.
//
// Every point has two ink densities (a and b). A field computes them as the
// product of its layers, so a layer can shape both inks ("ab") or only one.
// A layer reads one coordinate of the plane and passes it through a profile:
// a clamped ramp for gradients and splits, or a periodic pulse for stripes.
// Narrow ramps and hard pulse edges give the sharp edges; wide ones give
// continuous gradients. A pattern overlays several fields like light
// (screen blend), for motifs that are a union rather than a product, such as
// rings around a grid.
//
// The fields are then sampled into dots on a jittered grid: each cell holds
// at most one dot, inked a or b with probability equal to the densities.

export type Channel = "a" | "b" | "ab";

export type Coordinate =
  /** Distance along a direction; 0° points right, 90° points down. */
  | { type: "linear"; angle: number }
  /** Distance from the centre. */
  | { type: "radial" }
  /** Chebyshev distance: concentric rectangles, `aspect` = width / height. */
  | { type: "box"; aspect?: number }
  /** 1 / box distance: a corridor seen head-on, bands crowd the vanishing point. */
  | { type: "depth"; aspect?: number }
  /** Distance from the box's diagonals, to open gaps at rectangle corners. */
  | { type: "diagonal"; aspect?: number };

export type Profile =
  /** 0 before `from`, 1 after `to`, eased by `gamma`. `from > to` descends. */
  | { type: "ramp"; from: number; to: number; gamma?: number }
  /**
   * Repeating band. `duty` is the lit share of each period, `soft` the share
   * of the band spent fading in and out (0 = hard edges), `fade` dims the
   * band from its leading to its trailing edge (1 = to nothing; negative
   * values dim the leading edge instead).
   */
  | {
      type: "pulse";
      period: number;
      phase?: number;
      duty?: number;
      soft?: number;
      fade?: number;
    };

export type Layer = {
  channel: Channel;
  coordinate: Coordinate;
  profile: Profile;
  /** Offset of this layer's origin from the pattern centre. */
  x?: number;
  y?: number;
  invert?: boolean;
  /** How strongly the layer applies: 0 = no effect, 1 = full. */
  amount?: number;
};

export type Field = {
  /** Which inks the field lays down; the other ink gets nothing from it. */
  inks?: Channel;
  layers: Layer[];
};

export type StipplePattern = {
  fields: Field[];
  inks: { a: string; b: string };
  /** Distance between grid cells, in pattern units. */
  spacing: number;
  /** Dot radius, in pattern units. */
  radius: number;
  /** 0 = dots on a regular grid, 1 = anywhere in their cell. */
  jitter?: number;
  /** Scales both densities before sampling. */
  gain?: number;
  seed?: number;
};

export type Densities = { a: number; b: number };

const clamp01 = (v: number) => (v < 0 ? 0 : v > 1 ? 1 : v);

function smoothstep(edge0: number, edge1: number, v: number) {
  if (edge0 === edge1) return v < edge0 ? 0 : 1;
  const t = clamp01((v - edge0) / (edge1 - edge0));
  return t * t * (3 - 2 * t);
}

function coordinate(c: Coordinate, x: number, y: number) {
  switch (c.type) {
    case "linear": {
      const r = (c.angle * Math.PI) / 180;
      return x * Math.cos(r) + y * Math.sin(r);
    }
    case "radial":
      return Math.hypot(x, y);
    case "box":
      return Math.max(Math.abs(x) / (c.aspect ?? 1), Math.abs(y));
    case "depth":
      return 1 / Math.max(Math.abs(x) / (c.aspect ?? 1), Math.abs(y), 1e-6);
    case "diagonal":
      return Math.abs(Math.abs(x) / (c.aspect ?? 1) - Math.abs(y));
  }
}

function profile(p: Profile, s: number) {
  if (p.type === "ramp") {
    const t = p.from === p.to ? (s < p.from ? 0 : 1) : clamp01((s - p.from) / (p.to - p.from));
    return p.gamma ? Math.pow(t, p.gamma) : t;
  }
  const u = s / p.period - (p.phase ?? 0);
  const f = u - Math.floor(u);
  const duty = p.duty ?? 0.5;
  if (f >= duty) return 0;
  const edge = ((p.soft ?? 0) * duty) / 2;
  const lit = smoothstep(0, edge, f) * (1 - smoothstep(duty - edge, duty, f));
  const fade = p.fade ?? 0;
  const along = f / duty;
  return lit * (fade >= 0 ? 1 - fade * along : 1 + fade * (1 - along));
}

function fieldAt(field: Field, x: number, y: number): Densities {
  let a = field.inks === "b" ? 0 : 1;
  let b = field.inks === "a" ? 0 : 1;
  for (const layer of field.layers) {
    const s = coordinate(layer.coordinate, x - (layer.x ?? 0), y - (layer.y ?? 0));
    let v = profile(layer.profile, s);
    if (layer.invert) v = 1 - v;
    v = 1 - (layer.amount ?? 1) * (1 - v);
    if (layer.channel !== "b") a *= v;
    if (layer.channel !== "a") b *= v;
  }
  return { a, b };
}

/** Ink densities (0–1 each) at a point in pattern units, centre at the origin. */
export function densitiesAt(fields: Field[], x: number, y: number): Densities {
  let a = 1;
  let b = 1;
  for (const field of fields) {
    const d = fieldAt(field, x, y);
    a *= 1 - d.a;
    b *= 1 - d.b;
  }
  return { a: 1 - a, b: 1 - b };
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

export type Dot = { x: number; y: number; ink: "a" | "b" };

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
  const { spacing, fields } = pattern;
  const jitter = pattern.jitter ?? 1;
  const gain = pattern.gain ?? 1;
  const seed = pattern.seed ?? 1;
  const i0 = Math.floor(bounds.left / spacing);
  const i1 = Math.ceil(bounds.right / spacing);
  const j0 = Math.floor(bounds.top / spacing);
  const j1 = Math.ceil(bounds.bottom / spacing);
  const r = [0, 0, 0];
  for (let j = j0; j <= j1; j++) {
    for (let i = i0; i <= i1; i++) {
      cellRandom(seed, i, j, r);
      const x = (i + 0.5 + (r[0] - 0.5) * jitter) * spacing;
      const y = (j + 0.5 + (r[1] - 0.5) * jitter) * spacing;
      const pick = r[2];
      const d = densitiesAt(fields, x, y);
      let a = clamp01(d.a * gain);
      let b = clamp01(d.b * gain);
      const sum = a + b;
      if (sum > 1) {
        a /= sum;
        b /= sum;
      }
      if (pick < a) emit({ x, y, ink: "a" });
      else if (pick < a + b) emit({ x, y, ink: "b" });
    }
  }
}
