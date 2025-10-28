import { Vec2, ViewBox } from "../types";

/**
 * Calculate the intersection point of a line and a boundary.
 * @param center - The origin of the line.
 * @param point - The end of the line to calculate the intersection for.
 * @param bounds - The bounds of the boundary.
 * @returns The intersection point.
 */
export function calculateBoundaryIntersection(
  center: Vec2,
  point: Vec2,
  bounds: ViewBox,
): Vec2 {
  // Calculate direction vector
  const dx = point.x - center.x;
  const dy = point.y - center.y;

  // Calculate all possible intersections
  let horizontalIntersection: Vec2 | null = null;
  let verticalIntersection: Vec2 | null = null;

  // Check horizontal bounds
  if (dx !== 0) {
    // Skip horizontal bounds check if line is vertical
    if (point.x < bounds.x) {
      const y = center.y + (dy * (bounds.x - center.x)) / dx;
      if (y >= bounds.y && y <= bounds.y + bounds.height) {
        horizontalIntersection = { x: bounds.x, y };
      }
    } else if (point.x > bounds.x + bounds.width) {
      const y = center.y + (dy * (bounds.x + bounds.width - center.x)) / dx;
      if (y >= bounds.y && y <= bounds.y + bounds.height) {
        horizontalIntersection = { x: bounds.x + bounds.width, y };
      }
    }
  }

  // Check vertical bounds
  if (dy !== 0) {
    // Skip vertical bounds check if line is horizontal
    if (point.y < bounds.y) {
      const x = center.x + (dx * (bounds.y - center.y)) / dy;
      if (x >= bounds.x && x <= bounds.x + bounds.width) {
        verticalIntersection = { x, y: bounds.y };
      }
    } else if (point.y > bounds.y + bounds.height) {
      const x = center.x + (dx * (bounds.y + bounds.height - center.y)) / dy;
      if (x >= bounds.x && x <= bounds.x + bounds.width) {
        verticalIntersection = { x, y: bounds.y + bounds.height };
      }
    }
  }

  // Choose the intersection point that's closest to the actual point
  if (horizontalIntersection && verticalIntersection) {
    const horizontalDist = Math.hypot(
      point.x - horizontalIntersection.x,
      point.y - horizontalIntersection.y,
    );
    const verticalDist = Math.hypot(
      point.x - verticalIntersection.x,
      point.y - verticalIntersection.y,
    );
    return horizontalDist < verticalDist
      ? horizontalIntersection
      : verticalIntersection;
  }

  return (
    horizontalIntersection ||
    verticalIntersection || {
      x: Math.max(bounds.x, Math.min(bounds.x + bounds.width, point.x)),
      y: Math.max(bounds.y, Math.min(bounds.y + bounds.height, point.y)),
    }
  );
}
