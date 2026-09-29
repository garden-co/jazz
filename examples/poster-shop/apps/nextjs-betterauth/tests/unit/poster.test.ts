import { describe, expect, it } from "vitest";
import { demoPoster } from "../../src/lib/demo-poster.js";
import {
  fillColor,
  paintOrder,
  parseSnapshot,
  reorder,
  resizeFromCorner,
  takeSnapshot,
} from "../../src/lib/poster.js";

function counterIds() {
  let next = 0;
  return () => `00000000-0000-4000-8000-${String(++next).padStart(12, "0")}`;
}

describe("demo poster seed", () => {
  it("is deterministic for a deterministic id source", () => {
    expect(demoPoster("Ada", counterIds())).toEqual(demoPoster("Ada", counterIds()));
  });

  it("puts every shape on a seeded layer and snapshots exactly the seed", () => {
    const poster = demoPoster("Ada", counterIds());
    const layerIds = new Set(poster.layers.map((layer) => layer.id));
    expect(poster.title).toBe("Ada's poster");
    expect(poster.shapes.every((shape) => layerIds.has(shape.layerId))).toBe(true);
    expect(poster.checkpoint.snapshot).toEqual(takeSnapshot(poster.layers, poster.shapes));
    expect(parseSnapshot(poster.checkpoint.snapshot)).toEqual(poster.checkpoint.snapshot);
  });
});

describe("paint order", () => {
  it("orders layers first, then shapes inside each layer", () => {
    const layers = [
      { id: "top", zIndex: 1 },
      { id: "bottom", zIndex: 0 },
    ];
    const shapes = [
      { id: "a", layerId: "top", zIndex: 0 },
      { id: "b", layerId: "bottom", zIndex: 5 },
      { id: "c", layerId: "bottom", zIndex: 1 },
    ];
    expect(
      paintOrder(layers, shapes).map(({ layer, shapes }) => [layer.id, shapes.map((s) => s.id)]),
    ).toEqual([
      ["bottom", ["c", "b"]],
      ["top", ["a"]],
    ]);
  });
});

describe("reorder", () => {
  it("swaps neighbours and renumbers duplicate indexes", () => {
    const items = [
      { id: "a", zIndex: 0 },
      { id: "b", zIndex: 0 },
      { id: "c", zIndex: 1 },
    ];
    expect(reorder(items, "a", "up")).toEqual([
      { id: "b", zIndex: 0 },
      { id: "a", zIndex: 1 },
      { id: "c", zIndex: 2 },
    ]);
    expect(reorder(items, "c", "up")).toEqual([]);
    expect(reorder(items, "a", "down")).toEqual([]);
  });
});

describe("resize", () => {
  it("keeps the opposite corner fixed for an unrotated shape", () => {
    const start = { x: 100, y: 100, width: 200, height: 100, rotation: 0 };
    expect(resizeFromCorner(start, "se", 50, 20)).toEqual({ ...start, width: 250, height: 120 });
    expect(resizeFromCorner(start, "nw", 50, 20)).toEqual({
      ...start,
      x: 150,
      y: 120,
      width: 150,
      height: 80,
    });
  });

  it("never collapses below the minimum size", () => {
    const start = { x: 0, y: 0, width: 40, height: 40, rotation: 0 };
    const next = resizeFromCorner(start, "se", -500, -500);
    expect(next.width).toBeGreaterThan(0);
    expect(next.height).toBeGreaterThan(0);
  });
});

describe("palette", () => {
  it("maps stored keys to tokens and never passes unknown values through", () => {
    expect(fillColor("red")).toBe("var(--color-data-red-4)");
    expect(fillColor("#ff0000")).toBe("var(--color-data-gray-5)");
  });
});

describe("snapshot parsing", () => {
  it("rejects malformed checkpoint JSON", () => {
    expect(parseSnapshot(null)).toBeNull();
    expect(parseSnapshot({ version: 2, layers: [], shapes: [] })).toBeNull();
    expect(
      parseSnapshot({ version: 1, layers: [], shapes: [{ id: "x", kind: "star" }] }),
    ).toBeNull();
  });
});
