import { describe, expect, it } from "vitest";
import { formatMutationFieldValue, parseMutationFieldValue } from "./row-mutation-form";

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

  it("parses BigInt values exactly and formats them as decimal text", () => {
    const parsed = parseMutationFieldValue({ type: "BigInt" }, "9007199254740993");

    expect(parsed).toBe(9007199254740993n);
    expect(formatMutationFieldValue(parsed)).toBe("9007199254740993");
    expect(parseMutationFieldValue({ type: "BigInt" }, "-9223372036854775808")).toBe(
      -9223372036854775808n,
    );
    expect(parseMutationFieldValue({ type: "BigInt" }, "9223372036854775807")).toBe(
      9223372036854775807n,
    );
  });

  it("rejects non-decimal and out-of-range BigInt values", () => {
    expect(() => parseMutationFieldValue({ type: "BigInt" }, "1e3")).toThrow(
      "Value must be a signed 64-bit integer.",
    );
    expect(() => parseMutationFieldValue({ type: "BigInt" }, "9223372036854775808")).toThrow(
      "Value must be a signed 64-bit integer.",
    );
    expect(() => parseMutationFieldValue({ type: "BigInt" }, "-9223372036854775809")).toThrow(
      "Value must be a signed 64-bit integer.",
    );
  });
});
