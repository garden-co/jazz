import { describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";

describe("typed migration object syntax", () => {
  it("serializes add, drop, and rename operations from the migrate object", () => {
    const migration = s.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table({
          email: s.string(),
          legacyPriority: s.int().optional(),
        }),
        todos: s.table({
          title: s.string(),
          done: s.boolean(),
        }),
      },
      to: {
        users: s.table({
          emailAddress: s.string(),
        }),
        todos: s.table({
          title: s.string(),
          done: s.boolean(),
          description: s.string().optional(),
          ownerId: s.ref("users").optional(),
        }),
      },
      migrate: {
        users: {
          emailAddress: s.renameFrom("email"),
          legacyPriority: s.drop.int({ backwardsDefault: null }),
        },
        todos: {
          description: s.add.string({ default: null }),
          ownerId: s.add.ref("users", { default: null }),
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

  it("serializes explicit table renames alongside target-table column operations", () => {
    const migration = s.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table({
          email: s.string(),
        }),
      },
      to: {
        people: s.table({
          emailAddress: s.string(),
          nickname: s.string().optional(),
        }),
      },
      renameTables: {
        people: s.renameTableFrom("users"),
      },
      migrate: {
        people: {
          emailAddress: s.renameFrom("email"),
          nickname: s.add.string({ default: null }),
        },
      },
    });

    expect(migration.forward).toEqual([
      {
        table: "people",
        renamedFrom: "users",
        operations: [
          {
            type: "rename",
            column: "email",
            value: "emailAddress",
          },
          {
            type: "introduce",
            column: "nickname",
            sqlType: "TEXT",
            value: null,
          },
        ],
      },
    ]);
  });

  it("typechecks migrate coverage and op shapes", () => {
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: s.table({
            title: s.string(),
          }),
        },
        to: {
          todos: s.table({
            title: s.string(),
            description: s.string().optional(),
          }),
        },
        migrate: {
          todos: {
            description: s.add.string({ default: null }),
          },
        },
      });

      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: s.table({
            title: s.string(),
          }),
        },
        to: {
          todos: s.table({
            title: s.string(),
            description: s.string().optional(),
          }),
        },
        migrate: {
          todos: {
            // @ts-expect-error added columns must use s.add.*(...) or s.renameFrom(...)
            description: s.drop.string({ backwardsDefault: null }),
          },
        },
      });

      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: s.table({
            title: s.string(),
          }),
        },
        to: {
          todos: s.table({
            title: s.string(),
            description: s.string(),
          }),
        },
        migrate: {
          todos: {
            // @ts-expect-error required added columns need a non-null default of the right type
            description: s.add.string({ default: null }),
          },
        },
      });

      // @ts-expect-error removed columns must be dropped or renamed from
      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table({
            email: s.string(),
          }),
        },
        to: {
          users: s.table({}),
        },
        migrate: {},
      });

      // @ts-expect-error s.renameFrom(...) must point at a removed column with the same type
      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table({
            email: s.string(),
          }),
        },
        to: {
          users: s.table({
            emailAddress: s.int(),
          }),
        },
        migrate: {
          users: {
            emailAddress: s.renameFrom("email"),
          },
        },
      });
    }
  });
});
