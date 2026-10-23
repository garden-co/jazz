import { expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { structuralSchemaHash } from "../dev/schema-utils.js";
import { schemaDefinitionToAst } from "../migrations.js";
import { schemaToWasm } from "../codegen/schema-reader.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";
import { usesCurrentMembership, usesEncryptionSpacePolicy } from "./encrypted-schema.js";

function definition() {
  return {
    projects: s.table({ title: s.string() }, {}),
    teams: s.table({ name: s.string() }, {}),
    notes: s
      .table(
        { projectId: s.uuid(), teamId: s.uuid(), body: s.string() },
        { project: s.rel("projects", "projectId"), team: s.rel("teams", "teamId") },
      )
      .encrypted({ space: "projectId", columns: ["body"] }),
    files: s
      .table({ projectId: s.uuid(), data: s.bytes() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["data"] }),
    messages: s
      .table({ teamId: s.uuid(), text: s.string() }, { team: s.rel("teams", "teamId") })
      .encrypted({ space: "teamId", columns: ["text"] }),
  };
}

it("infers each scope once, including siblings and tables with other references", () => {
  const app = s.defineApp(definition());
  expect(Object.keys(app.wasmSchema).filter((name) => name.startsWith("__e2ee_v1_"))).toEqual([
    "__e2ee_v1_group_members",
    "__e2ee_v1_space_members_projects",
    "__e2ee_v1_permission_spaces_projects",
    "__e2ee_v1_space_administration_projects",
    "__e2ee_v1_space_members_teams",
    "__e2ee_v1_permission_spaces_teams",
    "__e2ee_v1_space_administration_teams",
  ]);
  expectTypeOf<s.RowOf<typeof app.__e2ee_v1_space_members_projects>>().toMatchTypeOf<{
    scopeId: string;
  }>();
  expectTypeOf<s.RowOf<typeof app.__e2ee_v1_space_members_teams>>().toMatchTypeOf<{
    scopeId: string;
  }>();
  expect(app.__e2ee_v1_space_members_projects._table).toBe("__e2ee_v1_space_members_projects");
  expect(app.__e2ee_v1_space_members_teams._table).toBe("__e2ee_v1_space_members_teams");
  const sliceable = s.defineSliceableApp(definition());
  const slice = sliceable.slice("notes");
  expect(slice.__e2ee_v1_space_members_projects._schema).toBe(sliceable.wasmSchema);
  expect(slice.__e2ee_v1_group_members._table).toBe("__e2ee_v1_group_members");
  definePermissions(app, ({ policy: p }) => {
    p.notes.fromEncryptionSpace();
    p.files.fromEncryptionSpace();
  });
  expect(usesCurrentMembership(app.wasmSchema, "projects")).toBe(true);
  expect(usesCurrentMembership(app.wasmSchema, "teams")).toBe(false);
  expect(usesEncryptionSpacePolicy(app.wasmSchema, "messages")).toBe(false);
});

it("does not enable membership maintenance or require registration for explicit-policy apps", () => {
  const app = s.defineApp(definition());
  expect(usesEncryptionSpacePolicy(app.wasmSchema, "notes")).toBe(false);
  expect(usesCurrentMembership(app.wasmSchema)).toBe(false);
  definePermissions(app, ({ policy: p }) => p.notes.allowWrite().always());
  expect(usesCurrentMembership(app.wasmSchema, "projects")).toBe(false);
  expect(usesCurrentMembership(app.wasmSchema)).toBe(false);
});

it("preserves explicit policies when a compiled definition is reused or spread", () => {
  const source = definition();
  const names = Object.keys(source);
  s.defineApp(source);
  for (const app of [s.defineApp(source), s.defineApp({ ...source })]) {
    definePermissions(app, ({ policy: p }) => p.notes.allowWrite().always());
    expect(usesCurrentMembership(app.wasmSchema)).toBe(false);
  }
  expect(Object.keys(source)).toEqual(names);
});

it("keeps automatic, legacy explicit and migration schemas structurally identical", () => {
  const source = definition();
  const automatic = s.defineApp(source);
  const explicit = s.defineApp({
    ...encryptionSpacePolicySchema("projects"),
    ...encryptionSpacePolicySchema("teams"),
    ...source,
  });
  const hash = structuralSchemaHash(automatic.wasmSchema);
  expect(structuralSchemaHash(explicit.wasmSchema)).toBe(hash);
  expect(structuralSchemaHash(schemaToWasm(schemaDefinitionToAst(source)))).toBe(hash);
  expect(usesCurrentMembership(explicit.wasmSchema, "projects")).toBe(true);
});

it("rejects replacing managed membership tables", () => {
  expect(() =>
    s.defineApp({
      ...definition(),
      __e2ee_v1_space_members_projects: s.table({ scopeId: s.uuid() }, {}),
    }),
  ).toThrow('Cannot replace managed E2EE table "__e2ee_v1_space_members_projects"');
});

it("leaves ordinary applications free of encryption tables", () => {
  const app = s.defineApp({ projects: s.table({ title: s.string() }, {}) });
  expect(Object.keys(app.wasmSchema)).toEqual(["projects"]);
});
