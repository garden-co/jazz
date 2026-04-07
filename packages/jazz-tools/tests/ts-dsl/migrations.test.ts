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

  it("serializes table renames", () => {
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

  it("serializes table additions and removals", () => {
    const migration = s.defineMigration({
      fromHash: "aaaaaaaaaaaa",
      toHash: "bbbbbbbbbbbb",
      from: {
        users: s.table({
          email: s.string(),
        }),
        legacyProfiles: s.table({
          bio: s.string().optional(),
        }),
      },
      to: {
        users: s.table({
          email: s.string(),
        }),
        profiles: s.table({
          bio: s.string().optional(),
        }),
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
          age: s.int(),
        }),
      },
      renameTables: {
        people: s.renameTableFrom("users"),
      },
      migrate: {
        people: {
          emailAddress: s.renameFrom("email"),
          age: s.add.int({ default: 18 }),
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
      s.defineMigration({
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
          }),
        },
        createTables: {
          people: true,
        },
        dropTables: {
          users: true,
        },
        migrate: {
          people: {
            emailAddress: s.renameFrom("email"),
          },
        },
      });
    }).toThrow(/cannot have column operations when declared in createTables or dropTables/);
  });

  it("rejects explicit table renames that still do not match after applying column migrations", () => {
    expect(() => {
      // @ts-expect-error explicit table renames that still do not match after column migrations
      s.defineMigration({
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
            age: s.int(),
          }),
        },
        renameTables: {
          people: s.renameTableFrom("users"),
        },
        migrate: {
          people: {
            emailAddress: s.renameFrom("email"),
          },
        },
      });
    }).toThrow(
      "Table rename users -> people does not match the target table after applying its column migrations.",
    );
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

      // @ts-expect-error target-only tables must be declared in createTables
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
            email: s.string(),
          }),
          profiles: s.table({
            bio: s.string().optional(),
          }),
        },
      });

      // @ts-expect-error source-only tables must be declared in dropTables
      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          users: s.table({
            email: s.string(),
          }),
          legacyProfiles: s.table({
            bio: s.string().optional(),
          }),
        },
        to: {
          users: s.table({
            email: s.string(),
          }),
        },
      });

      // @ts-expect-error s.renameTableFrom(...) must point at a removed table with the same shape
      s.defineMigration({
        fromHash: "aaaaaaaaaaaa",
        toHash: "bbbbbbbbbbbb",
        from: {
          legacyUsers: s.table({
            email: s.json(),
          }),
        },
        to: {
          users: s.table({
            email: s.string(),
          }),
        },
        renameTables: {
          users: s.renameTableFrom("legacyUsers"),
        },
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
