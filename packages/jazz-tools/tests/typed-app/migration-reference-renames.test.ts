import { describe, expect, it } from "vitest";
import { compileTestApp } from "../../src/testing/test-app.js";

const person = { columns: { name: "string" } };

/**
 * The forward lens of a migration from `from` (a history version, whose
 * reference columns name their table with `references`, as versions carry
 * no relations) to the current `to`.
 */
function forwardLens(
  from: Record<string, unknown>,
  to: Record<string, unknown>,
  migration: Record<string, unknown>,
) {
  return compileTestApp({
    tables: to,
    history: { v1: { tables: from } },
    migrations: [{ from: "v1", ...migration }],
  }).schema.migrations[0]!.forward;
}

describe("migration references through table renames", () => {
  it("keeps scalar, optional and array references when their target table is renamed", () => {
    const forward = forwardLens(
      {
        users: person,
        notes: {
          columns: {
            ownerId: { type: "uuid", references: "users" },
            reviewerId: { type: "uuid", optional: true, references: "users" },
            memberIds: { array: { type: "uuid", references: "users" } },
          },
        },
      },
      {
        people: person,
        notes: {
          columns: { ownerId: "uuid", reviewerId: "uuid?", memberIds: { array: "uuid" } },
          relations: {
            owner: { rel: "people", column: "ownerId" },
            reviewer: { rel: "people", column: "reviewerId" },
            members: { rel: "people", column: "memberIds" },
          },
        },
      },
      { renameTables: { people: "users" } },
    );
    expect(forward).toEqual([
      { table: "people", added: false, removed: false, renamedFrom: "users", operations: [] },
    ]);
  });

  it("composes target and source table renames with renamed and added reference columns", () => {
    const forward = forwardLens(
      {
        users: person,
        notes: { columns: { authorId: { type: "uuid", references: "users" }, title: "string" } },
      },
      {
        people: person,
        messages: {
          columns: { ownerId: "uuid", title: "string", reviewerId: "uuid?" },
          relations: {
            owner: { rel: "people", column: "ownerId" },
            reviewer: { rel: "people", column: "reviewerId" },
          },
        },
      },
      {
        renameTables: { people: "users", messages: "notes" },
        migrate: {
          messages: {
            ownerId: { renameFrom: "authorId" },
            reviewerId: { add: "uuid", default: null },
          },
        },
      },
    );
    expect(forward).toEqual([
      {
        table: "messages",
        added: false,
        removed: false,
        renamedFrom: "notes",
        operations: [
          { type: "rename", column: "authorId", value: "ownerId" },
          {
            type: "introduce",
            column: "reviewerId",
            column_type: { type: "Uuid" },
            value: { type: "Null" },
          },
        ],
      },
      { table: "people", added: false, removed: false, renamedFrom: "users", operations: [] },
    ]);
  });

  it("keeps a self-reference when its table is renamed", () => {
    const forward = forwardLens(
      {
        categories: {
          columns: {
            name: "string",
            parentId: { type: "uuid", optional: true, references: "categories" },
          },
        },
      },
      {
        folders: {
          columns: { name: "string", parentId: "uuid?" },
          relations: { parent: { rel: "folders", column: "parentId" } },
        },
      },
      { renameTables: { folders: "categories" } },
    );
    expect(forward).toEqual([
      { table: "folders", added: false, removed: false, renamedFrom: "categories", operations: [] },
    ]);
  });

  it("does not treat a different table as the renamed reference target", () => {
    expect(() =>
      forwardLens(
        {
          users: person,
          teams: person,
          notes: { columns: { ownerId: { type: "uuid", references: "users" } } },
        },
        {
          people: person,
          teams: person,
          notes: {
            columns: { ownerId: "uuid" },
            relations: { owner: { rel: "teams", column: "ownerId" } },
          },
        },
        { renameTables: { people: "users" } },
      ),
    ).toThrow(
      /column notes\.ownerId moves its reference from users to teams, which is not supported/,
    );
  });
});
