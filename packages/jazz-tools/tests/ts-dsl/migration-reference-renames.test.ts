import { describe, expect, it } from "vitest";
import { schema as s, migration as m } from "../../src/index.js";

const person = s.table({ name: s.string() }, {});

describe("migration references through table renames", () => {
  it("keeps scalar, optional and array references when their target table is renamed", () => {
    const columns = {
      ownerId: s.uuid(),
      reviewerId: s.uuid().optional(),
      memberIds: s.array(s.uuid()),
    };
    const migration = m.defineMigration({
      from: {
        users: person,
        notes: s.table(columns, {
          owner: s.rel("users", "ownerId"),
          reviewer: s.rel("users", "reviewerId"),
          members: s.rel("users", "memberIds"),
        }),
      },
      to: {
        people: person,
        notes: s.table(columns, {
          owner: s.rel("people", "ownerId"),
          reviewer: s.rel("people", "reviewerId"),
          members: s.rel("people", "memberIds"),
        }),
      },
      renameTables: { people: m.renameTableFrom("users") },
    });
    expect(migration.forward).toEqual([{ table: "people", renamedFrom: "users", operations: [] }]);
  });

  it("composes target and source table renames with renamed and added reference columns", () => {
    const migration = m.defineMigration({
      from: {
        users: person,
        notes: s.table(
          { authorId: s.uuid(), title: s.string() },
          { author: s.rel("users", "authorId") },
        ),
      },
      to: {
        people: person,
        messages: s.table(
          { ownerId: s.uuid(), title: s.string(), reviewerId: s.uuid().optional() },
          {
            owner: s.rel("people", "ownerId"),
            reviewer: s.rel("people", "reviewerId"),
          },
        ),
      },
      renameTables: {
        people: m.renameTableFrom("users"),
        messages: m.renameTableFrom("notes"),
      },
      migrate: {
        messages: {
          ownerId: m.renameFrom("authorId"),
          reviewerId: m.add.ref("people", { default: null }),
        },
      },
    });
    expect(migration.forward).toEqual([
      { table: "people", renamedFrom: "users", operations: [] },
      {
        table: "messages",
        renamedFrom: "notes",
        operations: [
          { type: "rename", column: "authorId", value: "ownerId" },
          { type: "introduce", column: "reviewerId", sqlType: "UUID", value: null },
        ],
      },
    ]);
  });

  it("keeps a self-reference when its table is renamed", () => {
    const columns = { name: s.string(), parentId: s.uuid().optional() };
    const migration = m.defineMigration({
      from: { categories: s.table(columns, { parent: s.rel("categories", "parentId") }) },
      to: { folders: s.table(columns, { parent: s.rel("folders", "parentId") }) },
      renameTables: { folders: m.renameTableFrom("categories") },
    });
    expect(migration.forward).toEqual([
      { table: "folders", renamedFrom: "categories", operations: [] },
    ]);
  });

  it("does not treat a different table as the renamed reference target", () => {
    const config = {
      from: {
        users: person,
        teams: person,
        notes: s.table({ ownerId: s.uuid() }, { owner: s.rel("users", "ownerId") }),
      },
      to: {
        people: person,
        teams: person,
        notes: s.table({ ownerId: s.uuid() }, { owner: s.rel("teams", "ownerId") }),
      },
      renameTables: { people: m.renameTableFrom("users") },
    };
    expect(() => {
      // @ts-expect-error A table rename does not permit changing the referenced identity.
      m.defineMigration(config);
    }).toThrow(/same reference target/);
  });
});
