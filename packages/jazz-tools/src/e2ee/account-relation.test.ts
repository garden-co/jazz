import { expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { e2eeAccountRelation } from "./account-relation.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { translateQuery } from "../runtime/query-adapter.js";
import { structuralSchemaHash } from "../dev/schema-utils.js";
import { wasmSchemasEqual } from "../dev/schema-utils.js";
import { definePermissions } from "../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";

function application(derived = false) {
  return s.defineApp({
    ...(derived ? encryptionSpacePolicySchema("projects") : {}),
    projects: s.table(
      { name: s.string() },
      {
        creator: e2eeAccountRelation("$createdBy.account"),
        updater: e2eeAccountRelation("$updatedBy.account"),
      },
    ),
    notes: s
      .table(
        { projectId: s.uuid(), body: s.string() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
}

it("uses provenance account relationships without adding stored columns", () => {
  const app = application();
  expect(app.wasmSchema.projects!.columns.map((column) => column.name)).toEqual(["name"]);
  expect(app.wasmSchema.projects!.synthetic_references_v1).toEqual({
    "$createdBy.account": "__e2ee_account_identities",
    "$updatedBy.account": "__e2ee_account_identities",
  });
  expect(
    analyzeRelations(app.wasmSchema)
      .get("projects")
      ?.map((r) => [r.name, r.fromColumn]),
  ).toEqual([
    ["creator", "$createdBy.account"],
    ["updater", "$updatedBy.account"],
  ]);
  const query = app.projects.include({ creator: true, updater: true });
  type Row = s.RowOf<typeof query>;
  expectTypeOf<Row["creator"]>().toMatchTypeOf<{ id: string } | null>();
  expectTypeOf<Row["updater"]>().toMatchTypeOf<{ id: string } | null>();
  const runtime = translateQuery(query._build(), app.wasmSchema);
  expect(runtime).toContain("projects.$createdBy.account");
  expect(runtime).toContain("projects.$updatedBy.account");
  app.projects.hopTo("creator");
});

it("rejects non-UUID magic fields and conflicting synthetic targets", () => {
  for (const column of [
    "$createdBy",
    "$createdAt",
    "$createdBy.identity.subject",
    "$missing.account",
  ])
    expect(() => (s.table as any)({}, { creator: e2eeAccountRelation(column) })).toThrow(
      /unknown column/,
    );
  // @ts-expect-error The whole author is a record, not an account UUID.
  const invalid = () => s.table({}, { creator: e2eeAccountRelation("$createdBy") });
  expect(invalid).toThrow(/unknown column/);
  expect(() =>
    (s.table as any)(
      {},
      {
        creator: e2eeAccountRelation("$createdBy.account"),
        other: s.rel("other", "$createdBy.account"),
      },
    ),
  ).toThrow(/Conflicting/);
});

it("includes synthetic reference targets in schema identity while ignoring alias names", () => {
  const app = application();
  const original = structuralSchemaHash(app.wasmSchema);
  const renamed = structuredClone(app.wasmSchema);
  renamed.projects!.relations!.author = renamed.projects!.relations!.creator!;
  delete renamed.projects!.relations!.creator;
  expect(structuralSchemaHash(renamed)).toBe(original);
  renamed.projects!.synthetic_references_v1!["$createdBy.account"] = "projects";
  expect(structuralSchemaHash(renamed)).not.toBe(original);
  expect(wasmSchemasEqual(renamed, app.wasmSchema)).toBe(false);
  expect(
    structuralSchemaHash({
      accounts: { columns: [] },
      projects: {
        columns: [],
        synthetic_references_v1: {
          "$createdBy.account": "accounts",
          "$updatedBy.account": "accounts",
        },
      },
    }),
  ).toBe("09c26732b35bfa0537d12b1c6330d138df049c400550370f49a263d31f00ba4f");
});

it.each([false, true])(
  "compiles creator-owned space policies with derived permissions %s",
  async (derived) => {
    const app = application(derived);
    const permissions = definePermissions(app, ({ policy: p, session }) => {
      p.projects.allowRead.always();
      p.projects.allowInsert.always();
      if (derived) p.notes.fromEncryptionSpace();
      else {
        p.notes.allowRead.always();
        p.notes.allowWrite().always();
      }
      p.notes.space.allowRead.always();
      p.notes.space.allowInsert.where((space) =>
        p.projects.existsIncludingCreated.where({
          id: space.identifier,
          "$createdBy.account": space.creatorAccountId,
        }),
      );
      p.notes.space.allowUpdate.where((space) =>
        p.projects.existsIncludingCreated.where({
          id: space.identifier,
          "$createdBy.account": session.user.account,
        }),
      );
    });
    const compiled = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);
    expect(JSON.stringify(compiled.__e2ee_spaces!.policies!.insert)).toContain(
      "$createdBy.account",
    );
  },
);
