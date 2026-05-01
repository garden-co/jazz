import { describe, expect, it } from "vitest";
import { collectSharpTurnPausePoints } from "../../docs/components/playgrounds/svg-plotter-geometry";

describe("collectSharpTurnPausePoints", () => {
  it("adds a linger point at a clear right-angle turn", () => {
    expect(
      collectSharpTurnPausePoints(
        [
          { x: 0, y: 0 },
          { x: 40, y: 0 },
          { x: 40, y: 40 },
        ],
        {
          minTurnAngleDegrees: 25,
        },
      ),
    ).toEqual([
      expect.objectContaining({
        x: 40,
        y: 0,
        lingerStrength: expect.any(Number),
        trailX: expect.any(Number),
        trailY: expect.any(Number),
      }),
    ]);
  });

  it("ignores a gentle bend", () => {
    expect(
      collectSharpTurnPausePoints(
        [
          { x: 0, y: 0 },
          { x: 40, y: 0 },
          { x: 80, y: 6 },
        ],
        {
          minTurnAngleDegrees: 25,
        },
      ),
    ).toEqual([]);
  });

  it("collapses very nearby sharp points into a single linger point", () => {
    expect(
      collectSharpTurnPausePoints(
        [
          { x: 0, y: 0 },
          { x: 40, y: 0 },
          { x: 40, y: 4 },
          { x: 36, y: 4 },
        ],
        {
          minSpacing: 6,
          minTurnAngleDegrees: 25,
        },
      ),
    ).toHaveLength(1);
  });

  it("finds corners in a closed rectangle loop", () => {
    expect(
      collectSharpTurnPausePoints(
        [
          { x: 0, y: 0 },
          { x: 40, y: 0 },
          { x: 40, y: 20 },
          { x: 0, y: 20 },
        ],
        {
          closed: true,
          minTurnAngleDegrees: 25,
        },
      ),
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ x: 0, y: 0, lingerStrength: expect.any(Number) }),
        expect.objectContaining({ x: 40, y: 0, lingerStrength: expect.any(Number) }),
        expect.objectContaining({ x: 40, y: 20, lingerStrength: expect.any(Number) }),
        expect.objectContaining({ x: 0, y: 20, lingerStrength: expect.any(Number) }),
      ]),
    );
  });
});
