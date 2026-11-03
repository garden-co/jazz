import { describe, expect, it } from "vitest";
import { safeColor, samePresence, type SelectionPresence } from "./presence.js";

describe("safeColor", () => {
  it("keeps colours", () => {
    for (const color of ["#146aff", "teal", "rgb(20 106 255 / 50%)", "var(--color-icon-blue)"]) {
      expect(safeColor(color)).toBe(color);
    }
  });

  it("draws anything else grey, so it adds no style of its own", () => {
    for (const color of [
      "red; background-image: url(https://example.com/x)",
      "red) ; position: fixed",
      "var(--x); display:none",
      "",
    ]) {
      expect(safeColor(color)).toBe("gray");
    }
  });
});

describe("samePresence", () => {
  const at = (position: number, before = "the windows ", after = "fogged and gold") =>
    ({ cursor: { side: "before", position, before, after } }) as unknown as SelectionPresence;

  it("counts only the text around it, not where that text now is", () => {
    expect(samePresence(at(10), at(42))).toBe(true);
    expect(samePresence(at(10), at(10, "the window"))).toBe(false);
    expect(samePresence(undefined, at(10))).toBe(false);
  });
});
