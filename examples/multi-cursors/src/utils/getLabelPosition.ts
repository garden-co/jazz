import { Vec2, ViewBox } from "../types";

interface TextDimensions {
  width: number;
  height: number;
}

interface LabelPosition {
  x: number;
  y: number;
}

/**
 * Calculate the position of a cursor label based on cursor position, label dimensions, and bounds
 * Such that the label is always on the same side of the bounds as the cursor
 * @param position - The cursor position
 * @param dimensions - The dimensions of the label text
 * @param bounds - The viewport bounds
 * @param isOutOfBounds - Whether the cursor is outside the bounds
 * @returns The calculated label position
 */
export function getLabelPosition(
  position: Vec2,
  dimensions: TextDimensions,
  bounds?: ViewBox,
  isOutOfBounds?: boolean,
): LabelPosition {
  if (!isOutOfBounds || !bounds) {
    return { x: position.x + 15, y: position.y + 25 };
  }

  // Calculate the percentage of the bounds that the intersection point is from the left
  const percentageH = (position.x - bounds.x) / bounds.width;
  const percentageV = (position.y - bounds.y) / bounds.height;

  return {
    x: position.x - percentageH * dimensions.width,
    y: position.y - percentageV * dimensions.height,
  };
}
