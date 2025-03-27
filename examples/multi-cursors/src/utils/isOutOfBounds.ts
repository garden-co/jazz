import { Vec2, ViewBox } from "../types";

/**
 * Check if a position is out of bounds of a view box.
 * @param position - The position to check.
 * @param bounds - The bounds of the view box.
 * @param grace - The grace distance to allow for the position to be out of bounds.
 * @returns True if the position is out of bounds, false otherwise.
 */
export function isOutOfBounds(
  position: Vec2,
  bounds: ViewBox,
  grace: number = 0,
): boolean {
  return (
    position.x < bounds.x - grace ||
    position.x > bounds.x + bounds.width + grace ||
    position.y < bounds.y - grace ||
    position.y > bounds.y + bounds.height + grace
  );
}
