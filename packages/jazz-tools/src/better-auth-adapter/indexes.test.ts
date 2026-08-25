import { describe, expect, it } from "vitest";
import { readTableIndexes } from "./indexes.js";

describe("readTableIndexes", () => {
  it("treats missing index metadata as no composite indexes", () => {
    expect(readTableIndexes("account", undefined)).toEqual([]);
  });

  it("rejects non-array metadata with an adapter diagnostic", () => {
    expect(() => readTableIndexes("account", { fields: ["issuer"] })).toThrow(
      '[Jazz Better Auth adapter] Invalid index metadata for table "account": expected indexes to be an array.',
    );
  });

  it("rejects a unique index without string fields before normal writes can TypeError", () => {
    expect(() => readTableIndexes("account", [{ unique: true }])).toThrow(
      '[Jazz Better Auth adapter] Invalid index metadata for table "account": index 0 must declare a non-empty string[] fields.',
    );
  });

  it("rejects sparse index metadata before normal writes can TypeError", () => {
    expect(() => readTableIndexes("account", Array(1))).toThrow(
      '[Jazz Better Auth adapter] Invalid index metadata for table "account": index 0 must be present.',
    );
  });

  it("rejects sparse index fields before normal writes can TypeError", () => {
    expect(() => readTableIndexes("account", [{ fields: Array(1), unique: true }])).toThrow(
      '[Jazz Better Auth adapter] Invalid index metadata for table "account": index 0 must declare a non-empty string[] fields.',
    );
  });

  it("preserves valid Better Auth composite unique indexes", () => {
    expect(
      readTableIndexes("account", [{ fields: ["issuer", "accountId"], unique: true }]),
    ).toEqual([{ fields: ["issuer", "accountId"], unique: true }]);
  });
});
