import { beforeEach, describe, expect, it } from "vitest";
import { generateHeaderId } from "../header-ids.mjs";

describe("generateHeaderId", () => {
  it("generates kebab-case IDs from text", () => {
    expect(generateHeaderId("Simple Header")).toBe("simple-header");
    expect(generateHeaderId("Complex Header With 123 Numbers")).toBe(
      "complex-header-with-123-numbers",
    );
  });

  it("handles code blocks in headers", () => {
    expect(generateHeaderId("Header with `code`")).toBe("header-with-code");
    expect(generateHeaderId("`Code` at start")).toBe("code-at-start");
    expect(generateHeaderId("Code at `end`")).toBe("code-at-end");
  });

  it("removes framework visibility markers", () => {
    expect(generateHeaderId("Header [!framework=react]")).toBe("header");
    expect(generateHeaderId("Header with `code` [!framework=react,vue]")).toBe(
      "header-with-code",
    );
  });

  it("handles special characters", () => {
    // The github-slugger package keeps underscores but removes other special chars
    expect(generateHeaderId("Header with !@#$%^&*()_+")).toBe("header-with-_");
    expect(generateHeaderId("Header with — em-dash")).toBe(
      "header-with--em-dash",
    );
  });
});
