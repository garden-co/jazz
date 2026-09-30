import { describe, expect, it } from "vitest";
import { densitiesAt, stipple, type Dot, type StipplePattern } from "./stipple";
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

describe("densitiesAt", () => {
  it("multiplies layers within a field and only inks the field's channels", () => {
    const d = densitiesAt(
      [
        {
          inks: "a",
          layers: [
            {
              channel: "ab",
              coordinate: { type: "linear", angle: 0 },
              profile: { type: "ramp", from: 0, to: 1 },
            },
            {
              channel: "a",
              coordinate: { type: "linear", angle: 90 },
              profile: { type: "ramp", from: 0, to: 1 },
            },
          ],
        },
      ],
      0.5,
      0.5,
    );
    expect(d.a).toBeCloseTo(0.25);
    expect(d.b).toBe(0);
  });

  it("overlays fields with a screen blend", () => {
    const half = {
      layers: [
        {
          channel: "ab" as const,
          coordinate: { type: "radial" as const },
          profile: { type: "ramp" as const, from: 0, to: 2 },
        },
      ],
    };
    const d = densitiesAt([half, half], 1, 0);
    expect(d.a).toBeCloseTo(0.75);
    expect(d.b).toBeCloseTo(0.75);
  });

  it("gives hard edges for a zero-width ramp and a hard pulse", () => {
    const edge = (x: number) =>
      densitiesAt(
        [
          {
            layers: [
              {
                channel: "ab",
                coordinate: { type: "linear", angle: 0 },
                profile: { type: "ramp", from: 0.3, to: 0.3 },
              },
              {
                channel: "ab",
                coordinate: { type: "linear", angle: 0 },
                profile: { type: "pulse", period: 1, duty: 0.5 },
              },
            ],
          },
        ],
        x,
        0,
      ).a;
    expect(edge(0.29)).toBe(0);
    expect(edge(0.31)).toBe(1);
    expect(edge(0.49)).toBe(1);
    expect(edge(0.51)).toBe(0);
  });

  it("keeps the presets' densities within 0 and 1", () => {
    for (const pattern of [gridPattern, stripePattern]) {
      for (let x = -1.2; x <= 1.2; x += 0.037) {
        for (let y = -1; y <= 1; y += 0.041) {
          const d = densitiesAt(pattern.fields, x, y);
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

  it("puts at most one dot in each cell", () => {
    const dots = collect({ ...gridPattern, gain: 10 }, -0.3, -0.3, 0.3, 0.3);
    const cells = new Set(
      dots.map(
        (d) => `${Math.floor(d.x / gridPattern.spacing)},${Math.floor(d.y / gridPattern.spacing)}`,
      ),
    );
    expect(cells.size).toBe(dots.length);
  });
});
