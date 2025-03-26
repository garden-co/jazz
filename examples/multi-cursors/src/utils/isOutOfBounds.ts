import { Vec2, ViewBox } from "../types";

export function isOutOfBounds(position: Vec2, bounds: ViewBox): boolean {
  return (
    position.x < bounds.x ||
    position.x > bounds.x + bounds.width ||
    position.y < bounds.y ||
    position.y > bounds.y + bounds.height
  );
}
