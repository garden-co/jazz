import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { migration as m } from "../migration-namespace.js";
import type { DefinedMigration } from "../migrations.js";
import { schemaDefinitionToAst } from "../migrations.js";
import { schemaToWasm } from "../codegen/schema-reader.js";
import { MissingMigrationError, resolveCanonicalMigrationForward } from "../dev/catalogue.js";
import { structuralSchemaHash } from "../dev/schema-utils.js";

function definition() {
  return {
    projects: s.table({ title: s.string() }, {}),
    teams: s.table({ name: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
    messages: s
      .table({ teamId: s.uuid(), text: s.string() }, { team: s.rel("teams", "teamId") })
      .encrypted({ space: "teamId", columns: ["text"] }),
  };
}

function canonical(migration: DefinedMigration) {
  const fromSchema = schemaToWasm(schemaDefinitionToAst(migration.from));
  const toSchema = schemaToWasm(schemaDefinitionToAst(migration.to));
  return {
    fromSchema,
    toSchema,
    fromHash: structuralSchemaHash(fromSchema),
    toHash: structuralSchemaHash(toSchema),
  };
}

it("removes a disappearing scope's generated tables while retaining the other scope", () => {
  const from = definition();
  const to = { projects: from.projects, teams: from.teams, messages: from.messages };
  const migration = m.defineMigration({ from, to, dropTables: { notes: true } });
  const schemas = canonical(migration);
  expect(resolveCanonicalMigrationForward(migration, schemas)).toEqual([
    { table: "notes", removed: true, operations: [] },
    { table: "__e2ee_v1_space_members_projects", removed: true, operations: [] },
    { table: "__e2ee_v1_permission_spaces_projects", removed: true, operations: [] },
    { table: "__e2ee_v1_space_administration_projects", removed: true, operations: [] },
  ]);
  expect(() => resolveCanonicalMigrationForward(undefined, schemas)).toThrow(MissingMigrationError);
});

it("removes all implicit managed tables when the last encrypted tables are dropped", () => {
  const from = definition();
  const to = { projects: from.projects, teams: from.teams };
  const migration = m.defineMigration({ from, to, dropTables: { notes: true, messages: true } });
  const schemas = canonical(migration);
  const forward = resolveCanonicalMigrationForward(migration, schemas);
  expect(forward.every((lens) => lens.removed && lens.operations.length === 0)).toBe(true);
  const dropped = forward.map((lens) => lens.table);
  expect(new Set(dropped).size).toBe(dropped.length);
  expect(dropped.sort()).toEqual(
    Object.keys(schemas.fromSchema)
      .filter((table) => !Object.hasOwn(schemas.toSchema, table))
      .sort(),
  );
});

it("retains shared scope tables while an encrypted sibling remains", () => {
  const source = definition();
  const from = { ...source, drafts: source.notes };
  const to = {
    projects: from.projects,
    teams: from.teams,
    messages: from.messages,
    drafts: from.drafts,
  };
  const migration = m.defineMigration({ from, to, dropTables: { notes: true } });
  expect(resolveCanonicalMigrationForward(migration, canonical(migration))).toEqual([
    { table: "notes", removed: true, operations: [] },
  ]);
});

it("preserves generated-table lineage when a scope is renamed instead of removed", () => {
  const from = definition();
  const to = {
    workspaces: from.projects,
    teams: from.teams,
    messages: from.messages,
    notes: s
      .table(
        { projectId: s.uuid(), body: s.string() },
        { project: s.rel("workspaces", "projectId") },
      )
      .encrypted({ space: "projectId", columns: ["body"] }),
  };
  const migration = m.defineMigration({
    from,
    to,
    renameTables: { workspaces: m.renameTableFrom("projects") },
  });
  const forward = resolveCanonicalMigrationForward(migration, canonical(migration));
  expect(forward.some((lens) => lens.removed || lens.added)).toBe(false);
  expect(forward.filter((lens) => lens.table.startsWith("__e2ee_v1_"))).toEqual([
    {
      table: "__e2ee_v1_space_administration_workspaces",
      renamedFrom: "__e2ee_v1_space_administration_projects",
      operations: [],
    },
    {
      table: "__e2ee_v1_space_members_workspaces",
      renamedFrom: "__e2ee_v1_space_members_projects",
      operations: [],
    },
    {
      table: "__e2ee_v1_permission_spaces_workspaces",
      renamedFrom: "__e2ee_v1_permission_spaces_projects",
      operations: [],
    },
  ]);
});
