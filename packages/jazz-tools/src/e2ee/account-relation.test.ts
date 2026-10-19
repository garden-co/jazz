import { expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { e2eeAccountRelation } from "./account-relation.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { translateQuery } from "../runtime/query-adapter.js";
import { definePermissions } from "../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";

function application(derived = false) {
  return s.defineApp({
    ...(derived ? encryptionSpacePolicySchema("projects") : {}),
    projects: s.table(
      { name: s.string(), creatorAccountId: s.uuid(), updaterAccountId: s.uuid() },
      {
        creator: e2eeAccountRelation("creatorAccountId"),
        updater: e2eeAccountRelation("updaterAccountId"),
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

it("uses stored UUID columns for account relationships", () => {
  const app = application();
  expect(app.wasmSchema.projects!.synthetic_references_v1).toBeUndefined();
  expect(
    analyzeRelations(app.wasmSchema)
      .get("projects")
      ?.map((r) => [r.name, r.fromColumn]),
  ).toEqual([
    ["creator", "creatorAccountId"],
    ["updater", "updaterAccountId"],
  ]);
  const query = app.projects.include({ creator: true, updater: true });
  type Row = s.RowOf<typeof query>;
  expectTypeOf<Row["creator"]>().toMatchTypeOf<{ id: string } | null>();
  expectTypeOf<Row["updater"]>().toMatchTypeOf<{ id: string } | null>();
  expect(translateQuery(query._build(), app.wasmSchema)).toContain("projects.creatorAccountId");
});

it.each(["$createdBy.account", "$updatedBy.account", "$createdBy", "$createdAt"])(
  "rejects unsupported provenance relationship %s before catalogue publication",
  (column) => expect(() => e2eeAccountRelation(column)).toThrow(/not supported.*stored UUID/),
);

it("also rejects provenance relationships declared with the generic relationship builder", () => {
  expect(() =>
    s.defineApp({
      __e2ee_account_identities: s.table({}, {}),
      projects: s.table({}, { creator: s.rel("__e2ee_account_identities", "$createdBy.account") }),
    }),
  ).toThrow(/not supported.*stored UUID/);
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
