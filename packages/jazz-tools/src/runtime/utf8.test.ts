import { afterEach, describe, expect, it, vi } from "vitest";
import { Utf8Decoder } from "./utf8.js";
const HostDecoder = globalThis.TextDecoder;
const encoder = new TextEncoder();
afterEach(() => vi.unstubAllGlobals());

describe("UTF-8 decoding on a host without TextDecoder", () => {
  it("preserves Unicode and initial BOM handling across independent calls", () => {
    vi.stubGlobal("TextDecoder", undefined);
    const decoder = new Utf8Decoder({ fatal: true });
    for (const text of [
      "",
      "plain ASCII",
      "café 日本語 🧭",
      "\ufeffnote",
      "a\ufeffb",
      "\ufeff\ufeffnote",
    ]) {
      const bytes = encoder.encode(text);
      expect(decoder.decode(bytes)).toBe(new HostDecoder("utf-8", { fatal: true }).decode(bytes));
    }
  });

  it("rejects invalid UTF-8 and preserves nonfatal maximal-subpart replacements", () => {
    vi.stubGlobal("TextDecoder", undefined);
    const invalid = [
      [0x80],
      [0xc0, 0xaf],
      [0xc2],
      [0xe0, 0x80, 0xaf],
      [0xed, 0xa0, 0x80],
      [0xf4, 0x90, 0x80, 0x80],
      [0xf5, 0x80, 0x80, 0x80],
      [0xe2, 0x82],
      [0xe2, 0x41, 0xc2, 0xa2],
      [0xf0, 0x90, 0x41],
      [0xff, 0xef, 0xbb, 0xbf],
    ];
    for (const data of invalid) {
      const bytes = Uint8Array.from(data);
      expect(() => new Utf8Decoder({ fatal: true }).decode(bytes)).toThrow(TypeError);
      expect(new Utf8Decoder().decode(bytes)).toBe(new HostDecoder().decode(bytes));
    }
  });

  it("decodes large text across bounded output chunks without losing code points", () => {
    const text = "\ufeff" + "Field notes café 🧭 日本語\n".repeat(100_000);
    const bytes = encoder.encode(text);
    vi.stubGlobal("TextDecoder", undefined);
    expect(new Utf8Decoder({ fatal: true }).decode(bytes)).toBe(text.slice(1));
  });

  it("agrees with the host decoder across every one- and two-byte input", () => {
    vi.stubGlobal("TextDecoder", undefined);
    const fallback = new Utf8Decoder();
    const native = new HostDecoder();
    for (let first = 0; first <= 255; first++) {
      expect(fallback.decode(Uint8Array.of(first))).toBe(native.decode(Uint8Array.of(first)));
      for (let second = 0; second <= 255; second++) {
        const bytes = Uint8Array.of(first, second);
        expect(fallback.decode(bytes)).toBe(native.decode(bytes));
      }
    }
  });
});
