import { describe, it, expect } from "vitest";
import { findNearestStop } from "../nearest-stop.js";

import type { GeoPoint, StopWithLocation } from "../nearest-stop.js";

describe("findNearestStop", () => {
  it("returns the nearest stop from multiple options (Paris → London is closer than Paris → Tokyo)", () => {
    const paris: GeoPoint = { lat: 48.8566, lng: 2.3522 };

    const stops: StopWithLocation[] = [
      { id: "london", lat: 51.5074, lng: -0.1278 }, // ~340 km
      { id: "tokyo", lat: 35.6762, lng: 139.6503 }, // ~9,700 km
      { id: "new-york", lat: 40.7128, lng: -74.006 }, // ~5,800 km
    ];

    const nearest = findNearestStop(paris, stops);

    expect(nearest).not.toBeNull();
    expect(nearest!.id).toBe("london");
  });

  it("returns null for an empty array", () => {
    const origin: GeoPoint = { lat: 51.5074, lng: -0.1278 };

    expect(findNearestStop(origin, [])).toBeNull();
  });

  it("returns the only stop when there is exactly one", () => {
    const origin: GeoPoint = { lat: -33.8688, lng: 151.2093 }; // Sydney

    const stops: StopWithLocation[] = [{ id: "auckland", lat: -36.8485, lng: 174.7633 }];

    const result = findNearestStop(origin, stops);

    expect(result).not.toBeNull();
    expect(result!.id).toBe("auckland");
    expect(result!.lat).toBe(-36.8485);
    expect(result!.lng).toBe(174.7633);
  });

  it("handles two stops at identical distances gracefully", () => {
    // Two points equidistant from the equator/prime-meridian origin
    const origin: GeoPoint = { lat: 0, lng: 0 };

    // Symmetric points — same latitude, mirrored longitude
    const stops: StopWithLocation[] = [
      { id: "east", lat: 0, lng: 10 },
      { id: "west", lat: 0, lng: -10 },
    ];

    const result = findNearestStop(origin, stops);

    // Must return one of the two, not crash or return null
    expect(result).not.toBeNull();
    expect(["east", "west"]).toContain(result!.id);
  });

  it("works across the antimeridian (lng 179 → lng -179 is nearby, not antipodal)", () => {
    // Point just west of the date line
    const nearDateLine: GeoPoint = { lat: 0, lng: 179 };

    const stops: StopWithLocation[] = [
      // Just east of the date line — ~220 km away across the antimeridian
      { id: "across-dateline", lat: 0, lng: -179 },
      // London — thousands of km away
      { id: "london", lat: 51.5074, lng: -0.1278 },
    ];

    const result = findNearestStop(nearDateLine, stops);

    expect(result).not.toBeNull();
    expect(result!.id).toBe("across-dateline");
  });

  it("works near the poles", () => {
    // A research station near the North Pole
    const arctic: GeoPoint = { lat: 89.9, lng: 0 };

    const stops: StopWithLocation[] = [
      // Svalbard — relatively close to the pole
      { id: "svalbard", lat: 78.2232, lng: 15.6267 },
      // Cape Town — far from the pole
      { id: "cape-town", lat: -33.9249, lng: 18.4241 },
    ];

    const result = findNearestStop(arctic, stops);

    expect(result).not.toBeNull();
    expect(result!.id).toBe("svalbard");
  });
});
