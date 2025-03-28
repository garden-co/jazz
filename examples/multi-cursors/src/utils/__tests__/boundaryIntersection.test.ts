import { describe, expect, it } from "vitest";
import { calculateBoundaryIntersection } from "../boundaryIntersection";

describe("calculateBoundaryIntersection", () => {
  const bounds = { x: 0, y: 0, width: 100, height: 100 };

  it("should handle vertical lines (dx = 0)", () => {
    const center = { x: 50, y: 50 };
    const point = { x: 50, y: 150 }; // Straight up from center

    const intersection = calculateBoundaryIntersection(center, point, bounds);

    expect(intersection).toEqual({ x: 50, y: 100 }); // Should intersect at bottom boundary
  });

  it("should handle horizontal lines (dy = 0)", () => {
    const center = { x: 50, y: 50 };
    const point = { x: 150, y: 50 }; // Straight right from center

    const intersection = calculateBoundaryIntersection(center, point, bounds);

    expect(intersection).toEqual({ x: 100, y: 50 }); // Should intersect at right boundary
  });

  it("should handle vertical lines at boundaries", () => {
    const center = { x: 0, y: 50 };
    const point = { x: 0, y: 150 }; // Vertical line at x=0

    const intersection = calculateBoundaryIntersection(center, point, bounds);

    expect(intersection).toEqual({ x: 0, y: 100 }); // Should intersect at bottom boundary
  });

  it("should handle horizontal lines at boundaries", () => {
    const center = { x: 50, y: 0 };
    const point = { x: 150, y: 0 }; // Horizontal line at y=0

    const intersection = calculateBoundaryIntersection(center, point, bounds);

    expect(intersection).toEqual({ x: 100, y: 0 }); // Should intersect at right boundary
  });
});
