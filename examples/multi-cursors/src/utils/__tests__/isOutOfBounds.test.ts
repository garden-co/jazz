import { describe, expect, it } from "vitest";
import { isOutOfBounds } from "../isOutOfBounds";

describe("isOutOfBounds", () => {
  it("should return true if the position is out of bounds", () => {
    expect(
      isOutOfBounds(
        { x: 101, y: 101 },
        { x: 0, y: 0, width: 100, height: 100 },
      ),
    ).toBe(true);
  });

  it("should return false if the position is within bounds", () => {
    expect(
      isOutOfBounds({ x: 50, y: 50 }, { x: 0, y: 0, width: 100, height: 100 }),
    ).toBe(false);
  });

  it("should return false if the position is inside the grace area", () => {
    expect(
      isOutOfBounds(
        { x: 110, y: 110 },
        { x: 0, y: 0, width: 100, height: 100 },
        20,
      ),
    ).toBe(false);
  });
});
