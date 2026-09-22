import { describe, expect, it } from "vitest";
import type { CompiledPermissionsMap } from "./schema-permissions.js";
import { normalizePermissionsForWasm } from "./schema-permissions.js";

describe("normalizePermissionsForWasm", () => {
  it("encodes raw permission literals into tagged wire values", () => {
    const permissions: CompiledPermissionsMap = {
      chats: {
        select: {
          using: {
            type: "Cmp",
            column: "isPublic",
            op: "Eq",
            value: {
              type: "Literal",
              value: true,
            },
          },
        },
      },
    };

    expect(normalizePermissionsForWasm(permissions)).toEqual({
      chats: {
        select: {
          using: {
            type: "Cmp",
            column: "isPublic",
            op: "Eq",
            value: {
              type: "Literal",
              value: {
                type: "Boolean",
                value: true,
              },
            },
          },
        },
        insert: undefined,
        update: undefined,
        delete: undefined,
      },
    });
  });

  it("encodes nested relation literals inside ExistsRel filters", () => {
    const permissions: CompiledPermissionsMap = {
      resources: {
        select: {
          using: {
            type: "ExistsRel",
            rel: {
              Filter: {
                input: {
                  TableScan: {
                    table: "resource_access_edges",
                  },
                },
                predicate: {
                  And: [
                    {
                      Cmp: {
                        left: {
                          scope: "resource_access_edges",
                          column: "kind",
                        },
                        op: "Eq",
                        right: {
                          Literal: "individual",
                        },
                      },
                    },
                    {
                      Cmp: {
                        left: {
                          scope: "resource_access_edges",
                          column: "grant_role",
                        },
                        op: "Eq",
                        right: {
                          Literal: "viewer",
                        },
                      },
                    },
                  ],
                },
              },
            },
          },
        },
      },
    };

    expect(normalizePermissionsForWasm(permissions)).toEqual({
      resources: {
        select: {
          using: {
            type: "ExistsRel",
            rel: {
              Filter: {
                input: {
                  TableScan: {
                    table: "resource_access_edges",
                  },
                },
                predicate: {
                  And: [
                    {
                      Cmp: {
                        left: {
                          scope: "resource_access_edges",
                          column: "kind",
                        },
                        op: "Eq",
                        right: {
                          Literal: {
                            type: "Text",
                            value: "individual",
                          },
                        },
                      },
                    },
                    {
                      Cmp: {
                        left: {
                          scope: "resource_access_edges",
                          column: "grant_role",
                        },
                        op: "Eq",
                        right: {
                          Literal: {
                            type: "Text",
                            value: "viewer",
                          },
                        },
                      },
                    },
                  ],
                },
              },
            },
          },
        },
        insert: undefined,
        update: undefined,
        delete: undefined,
      },
    });
  });

  it("encodes relation literals nested in EnumMatch payloads", () => {
    const normalized = normalizePermissionsForWasm({
      resources: {
        select: {
          using: {
            type: "ExistsRel",
            rel: {
              Filter: {
                input: { TableScan: { table: "resource_access_edges" } },
                predicate: {
                  EnumMatch: {
                    column: { column: "subject" },
                    case: "individual",
                    payload: {
                      Cmp: {
                        left: { column: "expiresAt" },
                        op: "Gt",
                        right: { Literal: new Date("2026-01-02T03:04:05.000Z") },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    });

    expect(normalized.resources?.select?.using).toMatchObject({
      rel: {
        Filter: {
          predicate: {
            EnumMatch: {
              payload: {
                Cmp: {
                  right: { Literal: { type: "Timestamp", value: 1_767_323_045_000 } },
                },
              },
            },
          },
        },
      },
    });
  });

  it.each([
    ["Date", new Date("2026-01-02T03:04:05.000Z"), { type: "Timestamp", value: 1_767_323_045_000 }],
    ["fractional number", 1.5, { type: "Double", value: 1.5 }],
    ["byte array", Uint8Array.of(1, 2, 3), { type: "Bytea", value: Uint8Array.of(1, 2, 3) }],
    [
      "array",
      ["owner", "editor"],
      {
        type: "Array",
        value: [
          { type: "Text", value: "owner" },
          { type: "Text", value: "editor" },
        ],
      },
    ],
  ])("encodes %s policy literals for core compilation", (_name, value, encoded) => {
    const normalized = normalizePermissionsForWasm({
      resources: {
        select: {
          using: {
            type: "Cmp",
            column: "access",
            op: "Eq",
            value: { type: "Literal", value },
          },
        },
      },
    });

    expect(normalized.resources?.select?.using).toMatchObject({
      value: { type: "Literal", value: encoded },
    });
  });

  it.each([
    ["invalid Date", new Date(Number.NaN), "Permissions policy Date literals must be valid."],
    [
      "pre-epoch Date",
      new Date("1969-12-31T23:59:59.999Z"),
      "Permissions policy Date literals must be on or after 1970-01-01T00:00:00.000Z.",
    ],
  ])("rejects %s values at the authoring boundary", (_name, value, error) => {
    expect(() =>
      normalizePermissionsForWasm({
        resources: {
          select: {
            using: {
              type: "Cmp",
              column: "occurredAt",
              op: "Eq",
              value: { type: "Literal", value },
            },
          },
        },
      }),
    ).toThrowError(error);
  });

  it.each([
    ["tagged", { type: "Timestamp", value: Number.NaN }],
    ["legacy tagged", { Timestamp: -1 }],
    ["nested tagged array", { type: "Array", value: [{ type: "Timestamp", value: -1 }] }],
    ["nested legacy array", { Array: [{ Timestamp: Number.NaN }] }],
  ])("does not let %s timestamp literals bypass range validation", (_name, value) => {
    expect(() =>
      normalizePermissionsForWasm({
        resources: {
          select: {
            using: {
              type: "Cmp",
              column: "occurredAt",
              op: "Eq",
              value: { type: "Literal", value },
            },
          },
        },
      }),
    ).toThrowError(
      "Permissions policy timestamp literals must be non-negative safe integer milliseconds.",
    );
  });

  it.each([
    ["tagged", { type: "Double", value: Number.NaN }],
    ["legacy tagged", { Double: Number.POSITIVE_INFINITY }],
    [
      "nested tagged array",
      { type: "Array", value: [{ type: "Double", value: Number.NEGATIVE_INFINITY }] },
    ],
    ["nested legacy array", { Array: [{ Double: Number.NaN }] }],
  ])("does not let %s floating-point literals bypass finite validation", (_name, value) => {
    expect(() =>
      normalizePermissionsForWasm({
        resources: {
          select: {
            using: {
              type: "Cmp",
              column: "ratio",
              op: "Eq",
              value: { type: "Literal", value },
            },
          },
        },
      }),
    ).toThrowError("Permissions policy floating-point literals must be finite numbers.");
  });

  it("rejects non-finite literals nested in EnumMatch payloads", () => {
    expect(() =>
      normalizePermissionsForWasm({
        resources: {
          select: {
            using: {
              type: "ExistsRel",
              rel: {
                Filter: {
                  input: { TableScan: { table: "resource_access_edges" } },
                  predicate: {
                    EnumMatch: {
                      column: { column: "subject" },
                      case: "individual",
                      payload: {
                        Cmp: {
                          left: { column: "ratio" },
                          op: "Eq",
                          right: { Literal: Number.NaN },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      }),
    ).toThrowError("Permissions policy floating-point literals must be finite numbers.");
  });
});
