import { describe, expect, it } from "vitest";
import {
  densitiesAt,
  expandCopies,
  layerAt,
  rotated,
  stipple,
  type Dot,
  type Layer,
  type StipplePattern,
} from "./stipple";
import { gridPattern, stripePattern } from "./stipple-presets";

const collect = (
  pattern: StipplePattern,
  left: number,
  top: number,
  right: number,
  bottom: number,
) => {
  const dots: Dot[] = [];
  stipple(pattern, { left, top, right, bottom }, (dot) => dots.push(dot));
  return dots;
};

const key = (dot: Dot) => `${dot.x.toFixed(9)},${dot.y.toFixed(9)},${dot.ink}`;

// Rises from 0 at x = 0 to 1 at x = 1.
const ramp: Layer = { ink: "a", source: { type: "linear", angle: 0, from: 0, to: 1 } };

describe("layerAt", () => {
  it("reads radial and linear gradients", () => {
    expect(layerAt(ramp, 0.25, 7)).toBeCloseTo(0.25);
    const radial: Layer = { ink: "b", source: { type: "radial", x: 1, y: 0, radius: 2 } };
    expect(layerAt(radial, 1, 0)).toBe(1);
    expect(layerAt(radial, 1, 1)).toBeCloseTo(0.5);
    expect(layerAt(radial, 5, 0)).toBe(0);
  });

  it("fluted glass repeats a stretched slice per rib, with sharp edges between ribs", () => {
    const fluted: Layer = {
      ...ramp,
      source: { type: "linear", angle: 0, from: -1, to: 1 },
      warps: [{ type: "flute", angle: 0, period: 0.2, scale: 8 }],
    };
    // Inside a rib the ramp runs from dark to bright ...
    expect(layerAt(fluted, 0.01, 0)).toBeLessThan(layerAt(fluted, 0.19, 0));
    // ... and neighbouring ribs show nearly the same slice (drifting by one
    // period of the source per rib), so it drops sharply at each edge.
    expect(layerAt(fluted, 0.199, 0) - layerAt(fluted, 0.201, 0)).toBeGreaterThan(0.6);
    expect(Math.abs(layerAt(fluted, 0.05, 0) - layerAt(fluted, 0.25, 0))).toBeLessThan(0.15);
  });

  it("a negative flute scale mirrors each rib, keeping its edges in place", () => {
    const flute = { type: "flute", angle: 0, period: 0.2, scale: 8 } as const;
    const source = { type: "linear", angle: 0, from: -1, to: 1 } as const;
    const up = layerAt({ ink: "a", source, warps: [flute] }, 0.05, 0);
    const down = layerAt({ ink: "a", source, warps: [{ ...flute, scale: -8 }] }, 0.15, 0);
    expect(down).toBeCloseTo(up);
  });

  it("rotates and mirrors the plane", () => {
    const [turned] = rotated([ramp], 90);
    expect(layerAt(turned, 0, 0.25)).toBeCloseTo(0.25);
    const mirrored: Layer = { ...ramp, warps: [{ type: "mirror", angle: 90 }] };
    expect(layerAt(mirrored, -0.25, 0)).toBeCloseTo(0.25);
  });
});

describe("expandCopies", () => {
  it("adds a turned copy of every layer", () => {
    const layers = expandCopies({ layers: [ramp], copies: [90] });
    expect(layers).toHaveLength(2);
    expect(layerAt(layers[1], 0, 0.25)).toBeCloseTo(0.25);
  });
});

describe("mirrored flutes", () => {
  it("meet at rib lines: one ink ends a rib where the other starts the next", () => {
    const flute = { type: "flute", angle: 0, period: 0.2, scale: 4, shift: 2 } as const;
    const source = { type: "radial", x: 0.9, radius: 3 } as const;
    const a: Layer = { ink: "a", source, warps: [flute] };
    const b: Layer = { ink: "b", source, warps: [{ ...flute, scale: -4 }] };
    // Within a rib, b reads what a reads at the mirrored position ...
    expect(layerAt(b, 0.25, 0)).toBeCloseTo(layerAt(a, 0.35, 0));
    // ... so at a rib line, b ends one rib and a starts the next on neighbouring
    // rib centres: nearly the same value, with both sharp edges meeting there.
    expect(Math.abs(layerAt(b, 0.3999, 0) - layerAt(a, 0.4001, 0))).toBeLessThan(0.1);
  });
});

describe("densitiesAt", () => {
  const half: Layer = { ink: "a", source: { type: "linear", angle: 0, from: 0, to: 1 } };

  it("combines same-ink layers by blend mode and leaves the other ink empty", () => {
    expect(densitiesAt([half, half], 0.5, 0, "screen")).toEqual({ a: 0.75, b: 0 });
    expect(densitiesAt([half, half], 0.5, 0, "max").a).toBeCloseTo(0.5);
    expect(densitiesAt([half, half], 0.5, 0, "multiply").a).toBeCloseTo(0.25);
  });

  it("keeps the presets' densities within 0 and 1", () => {
    for (const pattern of [gridPattern, stripePattern]) {
      for (let x = -1.2; x <= 1.2; x += 0.037) {
        for (let y = -1; y <= 1; y += 0.041) {
          const d = densitiesAt(pattern.layers, x, y, pattern.blend);
          for (const v of [d.a, d.b]) {
            expect(v).toBeGreaterThanOrEqual(0);
            expect(v).toBeLessThanOrEqual(1);
          }
        }
      }
    }
  });
});

describe("stipple", () => {
  it("places the same dots wherever the sampled rectangle falls", () => {
    const whole = new Set(collect(gridPattern, -0.4, -0.4, 0.4, 0.4).map(key));
    const part = collect(gridPattern, -0.1, -0.2, 0.3, 0.1);
    expect(part.length).toBeGreaterThan(100);
    for (const dot of part) expect(whole.has(key(dot))).toBe(true);
  });

  it("changes the dots with the seed", () => {
    const a = collect(stripePattern, -0.2, -0.2, 0.2, 0.2).map(key);
    const b = collect({ ...stripePattern, seed: 99 }, -0.2, -0.2, 0.2, 0.2).map(key);
    expect(a).not.toEqual(b);
  });

  it("puts at most one dot in each cell when sampling is shared", () => {
    const pattern: StipplePattern = { ...gridPattern, gain: 10, sampling: "shared" };
    const dots = collect(pattern, -0.3, -0.3, 0.3, 0.3);
    const cells = new Set(
      dots.map((d) => `${Math.floor(d.x / pattern.spacing)},${Math.floor(d.y / pattern.spacing)}`),
    );
    expect(cells.size).toBe(dots.length);
  });
});
