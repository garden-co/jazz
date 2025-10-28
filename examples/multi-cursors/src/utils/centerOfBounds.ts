import { Vec2, ViewBox } from "../types";

/**
 * Get the center of a bounds.
 * @param bounds - The bounds to get the center of.
 * @returns The center of the bounds.
 */
export function centerOfBounds(bounds?: ViewBox): Vec2 {
  if (!bounds) {
    return { x: 0, y: 0 };
  }

  return {
    x: bounds.x + bounds.width / 2,
    y: bounds.y + bounds.height / 2,
  };
}
