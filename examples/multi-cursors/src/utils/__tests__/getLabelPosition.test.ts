import { describe, expect, it } from "vitest";
import { getLabelPosition } from "../getLabelPosition";

describe("getLabelPosition", () => {
  const dimensions = { width: 100, height: 20 };
  const bounds = { x: 0, y: 0, width: 1000, height: 1000 };

  it("should position label with default offset when cursor is in bounds", () => {
    const position = { x: 500, y: 500 };
    const result = getLabelPosition(position, dimensions, bounds, false);

    expect(result).toEqual({
      x: position.x + 15,
      y: position.y + 25,
    });
  });

  it("should position label with default offset when bounds are undefined", () => {
    const position = { x: 500, y: 500 };
    const result = getLabelPosition(position, dimensions, undefined, true);

    expect(result).toEqual({
      x: position.x + 15,
      y: position.y + 25,
    });
  });

  it("should adjust label position based on cursor position when out of bounds", () => {
    const position = { x: 800, y: 600 };
    const result = getLabelPosition(position, dimensions, bounds, true);

    // At x=800, percentageH = 0.8, so x offset should be -80 (0.8 * width)
    // At y=600, percentageV = 0.6, so y offset should be -12 (0.6 * height)
    expect(result).toEqual({
      x: position.x - 0.8 * dimensions.width,
      y: position.y - 0.6 * dimensions.height,
    });
  });

  it("should handle cursor at bounds edges", () => {
    const position = { x: 1000, y: 1000 }; // Bottom-right corner
    const result = getLabelPosition(position, dimensions, bounds, true);

    // At the edges, percentages should be 1, so full dimension should be subtracted
    expect(result).toEqual({
      x: position.x - dimensions.width,
      y: position.y - dimensions.height,
    });
  });

  it("should handle cursor at bounds origin", () => {
    const position = { x: 0, y: 0 }; // Top-left corner
    const result = getLabelPosition(position, dimensions, bounds, true);

    // At origin, percentages should be 0, so no offset from position
    expect(result).toEqual({
      x: position.x,
      y: position.y,
    });
  });
});
