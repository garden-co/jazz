import { describe, it, expect } from "vitest";
import { computeRouteLine } from "../route-line.js";

describe("computeRouteLine", () => {
  it("produces a LineString with coordinates in chronological order for multiple stops", () => {
    const stops = [
      { date: new Date("2026-06-01"), lng: -0.1276, lat: 51.5074 }, // London
      { date: new Date("2026-06-05"), lng: 2.3522, lat: 48.8566 }, // Paris
      { date: new Date("2026-06-10"), lng: 13.405, lat: 52.52 }, // Berlin
    ];

    const result = computeRouteLine(stops);

    expect(result).toEqual({
      type: "LineString",
      coordinates: [
        [-0.1276, 51.5074],
        [2.3522, 48.8566],
        [13.405, 52.52],
      ],
    });
  });

  it("sorts stops by date regardless of input order", () => {
    const stops = [
      { date: new Date("2026-07-20"), lng: 23.7275, lat: 37.9838 }, // Athens
      { date: new Date("2026-07-01"), lng: -3.7038, lat: 40.4168 }, // Madrid
      { date: new Date("2026-07-10"), lng: 12.4964, lat: 41.9028 }, // Rome
    ];

    const result = computeRouteLine(stops);

    expect(result).toEqual({
      type: "LineString",
      coordinates: [
        [-3.7038, 40.4168], // Madrid (Jul 1)
        [12.4964, 41.9028], // Rome (Jul 10)
        [23.7275, 37.9838], // Athens (Jul 20)
      ],
    });
  });

  it("returns null for a single stop", () => {
    const stops = [
      { date: new Date("2026-09-15"), lng: -73.9857, lat: 40.7484 }, // New York
    ];

    expect(computeRouteLine(stops)).toBeNull();
  });

  it("returns null for an empty array", () => {
    expect(computeRouteLine([])).toBeNull();
  });

  it("produces a valid LineString when two stops share the same date", () => {
    const stops = [
      { date: new Date("2026-08-01"), lng: 4.9041, lat: 52.3676 }, // Amsterdam
      { date: new Date("2026-08-01"), lng: 4.3517, lat: 50.8503 }, // Brussels
    ];

    const result = computeRouteLine(stops);

    expect(result).not.toBeNull();
    expect(result!.type).toBe("LineString");
    expect(result!.coordinates).toHaveLength(2);

    // Both coordinates should be present (order is stable but unspecified)
    const coords = result!.coordinates as [number, number][];
    const asSet = new Set(coords.map((c) => c.join(",")));
    expect(asSet.has("4.9041,52.3676")).toBe(true);
    expect(asSet.has("4.3517,50.8503")).toBe(true);
  });
});
