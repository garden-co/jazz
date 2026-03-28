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
});
