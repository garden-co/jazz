import { expect, it } from "vitest";
import {
  checkRetainedEqualityCandidates,
  retainedEqualityLimits as limits,
} from "./retained-equality-limits.js";

it("refuses the complete sentinel result instead of truncating candidates", () => {
  expect(() =>
    checkRetainedEqualityCandidates(
      Array.from({ length: limits.rows + 1 }, () => ({ id: "row" })),
      1,
    ),
  ).toThrow("limit exceeded");
  expect(() =>
    checkRetainedEqualityCandidates([{ id: "row", value: new Uint8Array(64) }], 1),
  ).not.toThrow();
});
it("caps decryption cells independently of row count", () => {
  expect(() =>
    checkRetainedEqualityCandidates(
      Array.from({ length: limits.rows }, () => ({ id: "row" })),
      5,
    ),
  ).toThrow("limit exceeded");
});
it("checks every candidate cell including one beyond a requested page", () => {
  expect(() =>
    checkRetainedEqualityCandidates(
      [{ id: "first" }, { id: "later", value: new Uint8Array(limits.cellBytes + 1) }],
      1,
    ),
  ).toThrow("limit exceeded");
});
it("refuses aggregate bytes even when each cell fits", () => {
  expect(() =>
    checkRetainedEqualityCandidates(
      Array.from({ length: 5 }, () => ({ value: new Uint8Array(limits.cellBytes) })),
      1,
    ),
  ).toThrow("limit exceeded");
});
it("refuses excessive nesting without traversing unbounded values", () => {
  let nested: unknown = 1;
  for (let i = 0; i < limits.valueDepth + 2; i++) nested = { child: nested };
  expect(() => checkRetainedEqualityCandidates([{ value: nested }], 1)).toThrow("limit exceeded");
});
