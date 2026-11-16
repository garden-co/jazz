import { describe, expect, it } from "vitest";
import { asLines } from "../../src/lib/text.js";

describe("what the stop form saves", () => {
  it("ends a text with one line break", () => {
    expect(asLines("Doors at 8.  \n\n")).toBe("Doors at 8.\n");
    expect(asLines("Load-in at 2pm.\nSoundcheck at 5pm.")).toBe(
      "Load-in at 2pm.\nSoundcheck at 5pm.\n",
    );
  });

  it("saves an empty text as empty", () => {
    expect(asLines(" \n ")).toBe("");
    expect(asLines("")).toBe("");
  });
});
