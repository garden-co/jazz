import { describe, expect, it } from "vitest";
import { segmentRichText } from "../../../src/model/rich-text.js";

describe("segmentRichText", () => {
  it("uses UTF-8 byte offsets when extracting link labels", () => {
    const text = "👋 read example.com now";
    const label = "example.com";
    const byteStart = new TextEncoder().encode(text.slice(0, text.indexOf(label))).length;
    const byteEnd = byteStart + new TextEncoder().encode(label).length;

    expect(
      segmentRichText(
        text,
        JSON.stringify([{ byteStart, byteEnd, uri: "https://example.com/full-path" }]),
      ),
    ).toEqual([
      { text: "👋 read " },
      { text: label, uri: "https://example.com/full-path" },
      { text: " now" },
    ]);
  });
});
