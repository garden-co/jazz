import { describe, expect, it } from "vitest";
import fallback from "./fallback.json";
import { aboutKB, aboutMB, parseBundleSizes, replaceBundleSizeTokens } from "./index";

describe("bundle sizes", () => {
  it("accepts the committed fallback", () => {
    expect(parseBundleSizes(fallback)).not.toBeNull();
  });

  it("rejects malformed data", () => {
    expect(parseBundleSizes({ ...fallback, wasmGzipBytes: 0 })).toBeNull();
    expect(parseBundleSizes({ ...fallback, thinClientAddedGzipBytes: "150" })).toBeNull();
    expect(parseBundleSizes(null)).toBeNull();
  });

  it("rounds for prose", () => {
    expect(aboutKB(153_568)).toBe("about 150 KB");
    expect(aboutKB(156_000)).toBe("about 160 KB");
    expect(aboutMB(3_114_878)).toBe("about 3.1 MB");
    expect(aboutMB(3_000_000)).toBe("about 3 MB");
    expect(aboutMB(12_400_000)).toBe("about 12 MB");
  });

  it("replaces every token in a text", () => {
    const sizes = parseBundleSizes(fallback)!;
    expect(
      replaceBundleSizeTokens(
        "thin %bundle.thin%, Wasm %bundle.wasm% (%bundle.wasmRaw% raw)",
        sizes,
      ),
    ).toBe("thin about 150 KB, Wasm about 3.1 MB (8.8 MB raw)");
  });
});
