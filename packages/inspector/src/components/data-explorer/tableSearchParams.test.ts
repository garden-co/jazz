import { describe, expect, it } from "vitest";

import { DEFAULT_PAGE_SIZE, validateTableDataSearch } from "./tableSearchParams";

describe("validateTableDataSearch", () => {
  it("keeps valid non-default table search params", () => {
    expect(
      validateTableDataSearch({
        dir: "DESC",
        filters: [{ id: "name-filter", column: "name", operator: "contains", value: "Ada" }],
        page: "2",
        pageSize: "50",
        sort: "name",
      }),
    ).toEqual({
      dir: "DESC",
      filters: [{ id: "name-filter", column: "name", operator: "contains", value: "Ada" }],
      page: 2,
      pageSize: 50,
      sort: "name",
    });
  });

  it("compacts default values out of the URL state", () => {
    expect(
      validateTableDataSearch({
        dir: "ASC",
        page: "0",
        pageSize: String(DEFAULT_PAGE_SIZE),
        sort: "id",
      }),
    ).toEqual({});
  });

  it("supports legacy JSON-encoded filter search params", () => {
    expect(
      validateTableDataSearch({
        filters: JSON.stringify([
          { id: "relation-id-user-1", column: "id", operator: "eq", value: "user-1" },
        ]),
      }),
    ).toEqual({
      filters: [{ id: "relation-id-user-1", column: "id", operator: "eq", value: "user-1" }],
    });
  });

  it("drops malformed values and invalid filter operators", () => {
    expect(
      validateTableDataSearch({
        dir: "SIDEWAYS",
        filters: [
          { id: "valid", column: "name", operator: "eq", value: "Ada" },
          { id: "invalid", column: "name", operator: "startsWith", value: "A" },
        ],
        page: "not-a-page",
        pageSize: "13",
        sort: "",
      }),
    ).toEqual({
      filters: [{ id: "valid", column: "name", operator: "eq", value: "Ada" }],
    });
  });
});
