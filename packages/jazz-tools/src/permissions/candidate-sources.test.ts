import { describe, expect, it } from "vitest";
import { definePermissions } from "./index.js";
import { normalizePermissionsForWasm } from "../schema-permissions.js";

class Table<Row> {
  declare readonly _rowType: Row;
  where(_input: Partial<Row>): this {
    return this;
  }
}

const app = {
  roots: new Table<{ id: string; enabled: boolean }>(),
  parents: new Table<{ id: string; rootId: string; enabled: boolean }>(),
  children: new Table<{ id: string; parent: string }>(),
  wasmSchema: {
    roots: {
      columns: [
        { name: "id", column_type: { type: "Uuid" }, nullable: false },
        { name: "enabled", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
    parents: {
      columns: [
        { name: "id", column_type: { type: "Uuid" }, nullable: false },
        { name: "rootId", column_type: { type: "Uuid" }, nullable: false, references: "roots" },
        { name: "enabled", column_type: { type: "Boolean" }, nullable: false },
      ],
      relations: { root: { kind: "forward" as const, table: "roots", column: "rootId" } },
    },
    children: {
      columns: [
        { name: "id", column_type: { type: "Uuid" }, nullable: false },
        { name: "parent", column_type: { type: "Uuid" }, nullable: false, references: "parents" },
      ],
    },
  },
};

const correlation = {
  type: "Cmp",
  column: "id",
  op: "Eq",
  value: { type: "SessionRef", path: ["__jazz_outer_row", "parent"] },
};

describe("candidate permission sources", () => {
  it("keeps Alice's created source distinct from its ordinary sibling", () => {
    const permissions = definePermissions(app, ({ policy, allOf }) => {
      policy.children.allowInsert.where((child) =>
        allOf([
          policy.parents.existsIncludingCreated.where({ id: child.parent }),
          policy.parents.exists.where({ id: child.parent }),
        ]),
      );
    });
    expect(permissions.children?.insert?.with_check).toEqual({
      type: "And",
      exprs: [
        { type: "ExistsIncludingCreated", table: "parents", condition: correlation },
        { type: "Exists", table: "parents", condition: correlation },
      ],
    });
  });

  it("lowers Alice's nested literal for the runtime without losing the source marker", () => {
    const permissions = definePermissions(app, ({ policy }) => {
      policy.children.allowInsert.where((child) =>
        policy.parents.existsIncludingCreated.where({ id: child.parent, enabled: true }),
      );
    });
    expect(normalizePermissionsForWasm(permissions).children?.insert?.with_check).toEqual({
      type: "ExistsIncludingCreated",
      table: "parents",
      condition: {
        type: "And",
        exprs: [
          correlation,
          {
            type: "Cmp",
            column: "enabled",
            op: "Eq",
            value: { type: "Literal", value: { type: "Boolean", value: true } },
          },
        ],
      },
    });
  });

  it("refuses Mallory's relation traversal instead of dropping its created-source marker", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => {
        policy.children.allowInsert.where(
          policy.parents.existsIncludingCreated.where({ "roots.enabled": true } as never),
        );
      }),
    ).toThrow("existsIncludingCreated supports scalar correlated conditions");
  });
});
