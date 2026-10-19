import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";
import { e2eeAccountRelation } from "./account-relation.js";
import { spaceAdministrationTable } from "./space-administration-schema.js";
import { usesSpaceAdministration } from "./encrypted-schema.js";

function application(currentMembership = true) {
  return s.defineApp({
    ...(currentMembership ? encryptionSpacePolicySchema("projects") : {}),
    projects: s.table({ ownerId: s.uuid() }, {}),
    owners: s.table(
      { projectId: s.uuid(), accountId: s.uuid() },
      {
        project: s.rel("projects", "projectId"),
        account: e2eeAccountRelation(),
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
    files: s
      .table(
        { projectId: s.uuid(), data: s.bytes() },
        {
          project: s.rel("projects", "projectId"),
        },
      )
      .encrypted({ space: "projectId", columns: ["data"] }),
  });
}

it.each([false, true])(
  "compiles scoped administration alongside explicit or derived data policies (%s)",
  (derived) => {
    const app = application(derived);
    const permissions = definePermissions(app, ({ policy: p, session }) => {
      if (derived) p.notes.fromEncryptionSpace();
      else {
        p.notes.allowRead.always();
        p.notes.allowWrite().always();
      }
      p.notes.space.allowRead.always();
      p.notes.space.allowInsert.where((space) =>
        p.owners.existsIncludingCreated.where({
          accountId: space.accountId,
          projectId: space.identifier,
        }),
      );
      p.notes.space.allowUpdate.where((space) =>
        p.owners.existsIncludingCreated.where({
          accountId: session.user.account,
          projectId: space.identifier,
        }),
      );
    });
    const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);
    const roots = schema.__e2ee_spaces!.policies!;
    expect(JSON.stringify(roots.insert)).toContain('"__jazz_table_identity_v1","projects"');
    expect(roots.update!.with_check).toEqual({ type: "False" });
    expect(schema[spaceAdministrationTable("projects")]!.policies!.update!.with_check).toEqual({
      type: "False",
    });
    expect(JSON.stringify(schema.__e2ee_space_grants!.policies!.insert)).toContain(
      '"initialGrantId"',
    );
    for (const table of [
      "__e2ee_space_grants",
      "__e2ee_space_deliveries",
      "__e2ee_space_successors",
      "__e2ee_space_recovery_deliveries",
    ]) {
      expect(JSON.stringify(schema[table]!.policies!.insert)).toContain('"spaceId"');
      expect(schema[table]!.policies!.update!.with_check).toEqual({ type: "False" });
    }
    if (derived) {
      const members = schema.__e2ee_v1_space_members_projects!.policies!;
      expect(JSON.stringify(members.insert)).toContain('"__jazz_outer_row","scopeId"');
      expect(JSON.stringify(members.update!.using)).not.toContain("ExistsIncludingCreated");
      expect(JSON.stringify(members.update!.with_check)).not.toContain("ExistsIncludingCreated");
    }
    expect(usesSpaceAdministration(app.wasmSchema, "projects")).toBe(true);
  },
);

it("shares administration between sibling encrypted tables and rejects raw policy mixtures", () => {
  const app = application();
  const permissions = definePermissions(app, ({ policy: p }) => {
    p.notes.space.allowInsert.always();
    p.files.space.allowUpdate.always();
  });
  expect(permissions[spaceAdministrationTable("projects")]!.update!.using).toBeDefined();
  expect(
    Object.keys(permissions).filter((name) => name.startsWith("__e2ee_v1_space_administration_")),
  ).toHaveLength(1);
  for (const before of [true, false])
    expect(() =>
      definePermissions(app, ({ policy: p }) => {
        if (before) p.__e2ee_space_grants.allowInsert.always();
        p.notes.space.allowInsert.always();
        if (!before) p.__e2ee_space_grants.allowInsert.always();
      }),
    ).toThrow(/Cannot combine space administration helpers/);
  expect(() =>
    definePermissions(app, ({ policy: p }) => p.projects.space.allowInsert.always()),
  ).toThrow(/requires an encrypted table/);
});

it("rejects unsupported membership context instead of guessing a root creator", () => {
  const app = application();
  expect(() =>
    definePermissions(app, ({ policy: p, raw }) => {
      p.notes.space.allowUpdate.where(
        raw({
          type: "Cmp",
          column: "accountId",
          op: "Eq",
          value: { type: "Literal", value: crypto.randomUUID() },
        }),
      );
    }),
  ).toThrow(/only reference space.identifier/);
});

it("automatically validates account identity relationships while retaining unknown-target errors", () => {
  const app = application(false);
  expect(
    app.wasmSchema.owners!.columns.find((column) => column.name === "accountId")!.references,
  ).toBe("__e2ee_account_identities");
  expect(() => {
    // @ts-expect-error Ordinary apps still cannot reference an absent table.
    s.defineApp({ records: s.table({ accountId: s.uuid() }, { account: e2eeAccountRelation() }) });
  }).toThrow(/unknown table/);
});
