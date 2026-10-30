import { describe, expect, it } from "vitest";
import { schema as s, migration as m } from "../../src/index.js";

describe("typed migration object syntax", () => {
  it("serializes add, drop, and rename operations from the migrate object", () => {
    const migration = m.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table(
          {
            email: s.string(),
            legacyPriority: s.int().optional(),
          },
          {},
        ),
        todos: s.table(
          {
            title: s.string(),
            done: s.boolean(),
          },
          {},
        ),
      },
      to: {
        users: s.table(
          {
            emailAddress: s.string(),
          },
          { todosViaOwner: s.reverse("todos", "owner") },
        ),
        todos: s.table(
          {
            title: s.string(),
            done: s.boolean(),
            description: s.string().optional(),
            ownerId: s.uuid().optional(),
          },
          { owner: s.rel("users", "ownerId") },
        ),
      },
      migrate: {
        users: {
          emailAddress: m.renameFrom("email"),
          legacyPriority: m.drop.int({ backwardsDefault: null }),
        },
        todos: {
          description: m.add.string({ default: null }),
          ownerId: m.add.ref("users", { default: null }),
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

  it("serializes table renames", () => {
    const migration = m.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table(
          {
            email: s.string(),
          },
          {},
        ),
      },
      to: {
        people: s.table(
          {
            emailAddress: s.string(),
            nickname: s.string().optional(),
          },
          {},
        ),
      },
      renameTables: {
        people: m.renameTableFrom("users"),
      },
      migrate: {
        people: {
          emailAddress: m.renameFrom("email"),
          nickname: m.add.string({ default: null }),
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

  it("serializes table additions and removals", () => {
    const migration = m.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table(
          {
            email: s.string(),
          },
          {},
        ),
        legacyProfiles: s.table(
          {
            bio: s.string().optional(),
          },
          {},
        ),
      },
      to: {
        users: s.table(
          {
            email: s.string(),
          },
          {},
        ),
        profiles: s.table(
          {
            bio: s.string().optional(),
          },
          {},
        ),
      },
      createTables: {
        profiles: true,
      },
      dropTables: {
        legacyProfiles: true,
      },
    });

    expect(migration.forward).toEqual([
      {
        table: "profiles",
        added: true,
        operations: [],
      },
      {
        table: "legacyProfiles",
        removed: true,
        operations: [],
      },
    ]);
  });

  it("allows combining table renames with column migrations", () => {
    const migration = m.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table(
          {
            email: s.string(),
          },
          {},
        ),
      },
      to: {
        people: s.table(
          {
            emailAddress: s.string(),
            age: s.int(),
          },
          {},
        ),
      },
      renameTables: {
        people: m.renameTableFrom("users"),
      },
      migrate: {
        people: {
          emailAddress: m.renameFrom("email"),
          age: m.add.int({ default: 18 }),
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
            column: "age",
            sqlType: "INTEGER",
            value: 18,
          },
        ],
      },
    ]);
  });

  it("cannot combine createTables/dropTables with column migrations", () => {
    expect(() => {
      // @ts-expect-error cannot combine createTables/dropTables with column migrations
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
        to: {
          people: s.table(
            {
              emailAddress: s.string(),
            },
            {},
          ),
        },
        createTables: {
          people: true,
        },
        dropTables: {
          users: true,
        },
        migrate: {
          people: {
            emailAddress: m.renameFrom("email"),
          },
        },
      });
    }).toThrow(/cannot have column operations when declared in createTables or dropTables/);
  });

  it("rejects explicit table renames that still do not match after applying column migrations", () => {
    expect(() => {
      // @ts-expect-error explicit table renames that still do not match after column migrations
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
        to: {
          people: s.table(
            {
              emailAddress: s.string(),
              age: s.int(),
            },
            {},
          ),
        },
        renameTables: {
          people: m.renameTableFrom("users"),
        },
        migrate: {
          people: {
            emailAddress: m.renameFrom("email"),
          },
        },
      });
    }).toThrow(
      "Table rename users -> people does not match the target table after applying its column migrations.",
    );
  });

  it("typechecks migrate coverage and op shapes", () => {
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: s.table(
            {
              title: s.string(),
            },
            {},
          ),
        },
        to: {
          todos: s.table(
            {
              title: s.string(),
              description: s.string().optional(),
            },
            {},
          ),
        },
        migrate: {
          todos: {
            description: m.add.string({ default: null }),
          },
        },
      });

      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: s.table(
            {
              title: s.string(),
            },
            {},
          ),
        },
        to: {
          todos: s.table(
            {
              title: s.string(),
              description: s.string().optional(),
            },
            {},
          ),
        },
        migrate: {
          todos: {
            // @ts-expect-error added columns must use m.add.*(...) or m.renameFrom(...)
            description: m.drop.string({ backwardsDefault: null }),
          },
        },
      });

      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          todos: s.table(
            {
              title: s.string(),
            },
            {},
          ),
        },
        to: {
          todos: s.table(
            {
              title: s.string(),
              description: s.string(),
            },
            {},
          ),
        },
        migrate: {
          todos: {
            // @ts-expect-error required added columns need a non-null default of the right type
            description: m.add.string({ default: null }),
          },
        },
      });

      // @ts-expect-error removed columns must be dropped or renamed from
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
        to: {
          users: s.table({}, {}),
        },
        migrate: {},
      });

      // @ts-expect-error target-only tables must be declared in createTables
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
        to: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
          profiles: s.table(
            {
              bio: s.string().optional(),
            },
            {},
          ),
        },
      });

      // @ts-expect-error source-only tables must be declared in dropTables
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
          legacyProfiles: s.table(
            {
              bio: s.string().optional(),
            },
            {},
          ),
        },
        to: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
      });

      // @ts-expect-error m.renameTableFrom(...) must point at a removed table with the same shape
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          legacyUsers: s.table(
            {
              email: s.json(),
            },
            {},
          ),
        },
        to: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
        renameTables: {
          users: m.renameTableFrom("legacyUsers"),
        },
      });

      // @ts-expect-error m.renameFrom(...) must point at a removed column with the same type
      m.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table(
            {
              email: s.string(),
            },
            {},
          ),
        },
        to: {
          users: s.table(
            {
              emailAddress: s.int(),
            },
            {},
          ),
        },
        migrate: {
          users: {
            emailAddress: m.renameFrom("email"),
          },
        },
      });
    }
  });
});
