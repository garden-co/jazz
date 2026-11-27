import { describe, expect, it } from "vitest";
import { appendsEnumVariants } from "./sealed-enum.js";

describe("appendsEnumVariants", () => {
  const draft = { enum: ["draft"] };

  it("holds when variants are appended with the same nullability", () => {
    expect(appendsEnumVariants(draft, { enum: ["draft", "sent"] })).toBe(true);
    expect(
      appendsEnumVariants(
        { enum: ["draft"], optional: true },
        { enum: ["draft", "sent", "read"], optional: true },
      ),
    ).toBe(true);
    expect(appendsEnumVariants({ ...draft, optional: false }, { enum: ["draft", "sent"] })).toBe(
      true,
    );
  });

  it("is false for an enum that didn't grow", () => {
    expect(appendsEnumVariants(draft, { enum: ["draft"] })).toBe(false);
    expect(appendsEnumVariants({ enum: ["draft", "sent"] }, draft)).toBe(false);
  });

  it("is false when existing variants are reordered, renamed or inserted before", () => {
    expect(appendsEnumVariants({ enum: ["a", "b"] }, { enum: ["b", "a", "c"] })).toBe(false);
    expect(appendsEnumVariants({ enum: ["a", "b"] }, { enum: ["a", "c", "d"] })).toBe(false);
    expect(appendsEnumVariants(draft, { enum: ["new", "draft"] })).toBe(false);
  });

  it("is false when nullability changes", () => {
    expect(appendsEnumVariants(draft, { enum: ["draft", "sent"], optional: true })).toBe(false);
    expect(appendsEnumVariants({ ...draft, optional: true }, { enum: ["draft", "sent"] })).toBe(
      false,
    );
  });

  it("is false when either side isn't an enum", () => {
    expect(appendsEnumVariants({}, { enum: ["draft"] })).toBe(false);
    expect(appendsEnumVariants(draft, {})).toBe(false);
    expect(appendsEnumVariants({ enum: "draft" }, { enum: ["draft", "sent"] })).toBe(false);
  });
});
