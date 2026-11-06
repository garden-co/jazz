import { describe, expect, it } from "vitest";
import { textAnchorsFrom } from "./text-anchors.js";
import { loadWasmModule } from "./wasm-loader.js";

describe("text anchors through the WASM runtime", () => {
  it("captures an anchor and finds it after an edit before it", async () => {
    const text = textAnchorsFrom(await loadWasmModule());
    const value = "Load-in at 2pm. Doors open at 8pm.";
    const anchor = text.captureAnchor(value, value.indexOf("8pm"), "after", 1);
    expect(anchor).toMatchObject({ version: 1, side: "after", after: "8pm." });
    const next = "Load-in at 1pm sharp. Doors open at 8pm.";
    const found = text.resolveAnchor(anchor, next, 2);
    expect(found.position).toBe(next.indexOf("8pm"));
    expect(found.ambiguous).toBe(false);
    expect(found.confidence).toBeGreaterThan(0.9);
  });

  it("resolves an authored range with no version or position", async () => {
    const text = textAnchorsFrom(await loadWasmModule());
    const value = "Monday: Rehearsal at the studio. Tuesday: the studio is closed.";
    const found = text.resolveRange(
      { before: "Rehearsal at ", matched: "the studio", after: "." },
      value,
    );
    expect([found.start, found.end]).toEqual([
      value.indexOf("the studio"),
      value.indexOf("the studio") + 10,
    ]);
    expect(found.ambiguous).toBe(false);
  });

  it("counts positions in UTF-16 units, like JavaScript strings", async () => {
    const text = textAnchorsFrom(await loadWasmModule());
    const value = "🎸 Gear: Fender";
    const range = text.captureRange(value, value.indexOf("Fender"), value.length);
    expect(range.matched).toBe("Fender");
    expect(range.version).toBeUndefined();
    const found = text.resolveRange(range, "🎸🥁 Gear: Fender");
    expect(found.start).toBe("🎸🥁 Gear: ".length);
  });
});
