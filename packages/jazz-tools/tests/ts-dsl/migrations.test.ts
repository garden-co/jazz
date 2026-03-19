import { describe, expect, it } from "vitest";
import { col } from "../../src/dsl.js";
import { defineMigration } from "../../src/migrations.js";

describe("typed migration object syntax", () => {
  it("serializes add, drop, and rename operations from the migrate object", () => {
    const migration = defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: {
          email: col.string(),
          legacyPriority: col.int().optional(),
        },
        todos: {
          title: col.string(),
          done: col.boolean(),
        },
      },
      to: {
        users: {
          emailAddress: col.string(),
        },
        todos: {
          title: col.string(),
          done: col.boolean(),
          description: col.string().optional(),
          ownerId: col.ref("users").optional(),
        },
      },
      migrate: {
        users: {
          emailAddress: col.renameFrom("email"),
          legacyPriority: col.drop.int({ backwardsDefault: null }),
        },
        todos: {
          description: col.add.string({ default: null }),
          ownerId: col.add.ref("users", { default: null }),
        },
      },
    });

    expect(migration.forward).toEqual([
      {
        table: "users",
        operations: [
          {
            type: "rename",
            column: "email",
            value: "emailAddress",
          },
          {
            type: "drop",
            column: "legacyPriority",
            sqlType: "INTEGER",
            value: null,
          },
        ],
      },
      {
        table: "todos",
        operations: [
          {
            type: "introduce",
            column: "description",
            sqlType: "TEXT",
            value: null,
          },
          {
            type: "introduce",
            column: "ownerId",
            sqlType: "UUID",
            value: null,
          },
        ],
      },
    ]);
  });

  it("typechecks migrate coverage and op shapes", () => {
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: {
            title: col.string(),
          },
        },
        to: {
          todos: {
            title: col.string(),
            description: col.string().optional(),
          },
        },
        migrate: {
          todos: {
            description: col.add.string({ default: null }),
          },
        },
      });

      defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: {
            title: col.string(),
          },
        },
        to: {
          todos: {
            title: col.string(),
            description: col.string().optional(),
          },
        },
        migrate: {
          todos: {
            // @ts-expect-error added columns must use col.add.*(...) or col.renameFrom(...)
            description: col.drop.string({ backwardsDefault: null }),
          },
        },
      });

      defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: {
            title: col.string(),
          },
        },
        to: {
          todos: {
            title: col.string(),
            description: col.string(),
          },
        },
        migrate: {
          todos: {
            // @ts-expect-error required added columns need a non-null default of the right type
            description: col.add.string({ default: null }),
          },
        },
      });

      // @ts-expect-error removed columns must be dropped or renamed from
      defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: {
            email: col.string(),
          },
        },
        to: {
          users: {},
        },
        migrate: {},
      });

      // @ts-expect-error col.renameFrom(...) must point at a removed column with the same type
      defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: {
            email: col.string(),
          },
        },
        to: {
          users: {
            emailAddress: col.int(),
          },
        },
        migrate: {
          users: {
            emailAddress: col.renameFrom("email"),
          },
        },
      });
    }
  });
});
