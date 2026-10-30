import { describe, expect, it } from "vitest";
import { buildTourFixture, createRng, DEFAULT_SEED } from "../../src/fixture.js";

const start = new Date(2026, 8, 29, 9, 30);

describe("demo tour fixture", () => {
  it("is identical for the same seed and start date", () => {
    expect(buildTourFixture({ start })).toEqual(buildTourFixture({ seed: DEFAULT_SEED, start }));
  });

  it("changes with the seed", () => {
    const a = buildTourFixture({ seed: 1, start }).stops.map((s) => s.venue.name);
    const b = buildTourFixture({ seed: 2, start }).stops.map((s) => s.venue.name);
    expect(a).not.toEqual(b);
  });

  it("plans twelve evening shows on distinct days in the next three weeks, west to east", () => {
    const { stops } = buildTourFixture({ start });
    expect(stops).toHaveLength(12);

    const days = stops.map((s) => new Date(s.date).setHours(0, 0, 0, 0));
    expect(new Set(days).size).toBe(12);
    const first = new Date(2026, 8, 29).getTime();
    const last = new Date(2026, 9, 19).getTime();
    for (const day of days) {
      expect(day).toBeGreaterThanOrEqual(first);
      expect(day).toBeLessThanOrEqual(last);
    }
    for (const stop of stops) {
      expect(stop.date.getHours()).toBeGreaterThanOrEqual(18);
      expect(stop.date.getHours()).toBeLessThanOrEqual(21);
    }

    const lngs = stops.map((s) => s.venue.lng);
    expect(lngs).toEqual([...lngs].sort((a, b) => a - b));
    expect(stops[0].status).toBe("confirmed");
  });

  it("pins the default tour so fixture changes are deliberate", () => {
    const { stops } = buildTourFixture({ start });
    expect(stops.map((s) => `${s.date.getDate()} ${s.venue.city} ${s.status}`)).toMatchSnapshot();
  });

  it("uses a stable PRNG", () => {
    const rand = createRng(42);
    const first = [rand(), rand(), rand()];
    const again = createRng(42);
    expect([again(), again(), again()]).toEqual(first);
    for (const n of first) {
      expect(n).toBeGreaterThanOrEqual(0);
      expect(n).toBeLessThan(1);
    }
  });
});
