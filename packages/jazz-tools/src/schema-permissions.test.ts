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
});
