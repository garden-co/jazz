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
        insert: { with_check: { type: "False" } },
        update: {
          using: { type: "False" },
          with_check: { type: "False" },
        },
        delete: { using: { type: "False" } },
      },
    });
  });

  it("uses update.using as the enforcing delete fallback", () => {
    const permissions: CompiledPermissionsMap = {
      todos: {
        update: {
          using: {
            type: "Cmp",
            column: "ownerId",
            op: "Eq",
            value: { type: "SessionRef", path: ["user_id"] },
          },
        },
      },
    };

    const normalized = normalizePermissionsForWasm(permissions);
    expect(normalized.todos?.delete?.using).toEqual(normalized.todos?.update?.using);
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
        insert: { with_check: { type: "False" } },
        update: {
          using: { type: "False" },
          with_check: { type: "False" },
        },
        delete: { using: { type: "False" } },
      },
    });
  });
});
