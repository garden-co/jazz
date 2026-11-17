import { describe, expect, it } from "vitest";
import { assertUserColumnNameAllowed, assertUserTableColumnNameAllowed } from "./magic-columns.js";

describe("reserved column names", () => {
  // Rust's schema compile refuses these (`RESERVED_ROW_COLUMNS`); the DSL
  // refuses them too, instead of leaving it to the first compile.
  it.each(["rid", "_branch", "_clock", "_writer", "_state", "_base", "_touched", "_partition"])(
    "refuses the store's row column %s as a table column",
    (name) => {
      expect(() => assertUserTableColumnNameAllowed(name)).toThrow(
        "reserved for the store's own row columns",
      );
    },
  );

  it("refuses the aggregate-output namespace and keeps the existing refusals", () => {
    expect(() => assertUserTableColumnNameAllowed("__jazz_aggregate_count")).toThrow(
      "aggregate-output namespace",
    );
    expect(() => assertUserTableColumnNameAllowed("id")).toThrow("UUID row ID");
    expect(() => assertUserTableColumnNameAllowed("$createdAt")).toThrow("magic columns");
  });

  it("leaves ordinary and nested field names alone", () => {
    for (const name of ["title", "branch", "_title", "rid_count"]) {
      expect(() => assertUserTableColumnNameAllowed(name)).not.toThrow();
    }
    // Only a table's own columns share the row's namespace.
    expect(() => assertUserColumnNameAllowed("rid")).not.toThrow();
  });
});
