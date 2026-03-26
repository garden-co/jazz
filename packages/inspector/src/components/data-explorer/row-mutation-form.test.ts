import { describe, expect, it } from "vitest";
import { parseMutationFieldValue } from "./row-mutation-form";

describe("parseMutationFieldValue", () => {
  it("rejects empty integer input", () => {
    expect(() => parseMutationFieldValue({ type: "Integer" }, "   ")).toThrow("Value is required.");
  });

  it("rejects empty double input", () => {
    expect(() => parseMutationFieldValue({ type: "Double" }, "")).toThrow("Value is required.");
  });

  it("still parses explicit zero for numeric fields", () => {
    expect(parseMutationFieldValue({ type: "Integer" }, "0")).toBe(0);
    expect(parseMutationFieldValue({ type: "Double" }, "0")).toBe(0);
  });
});
