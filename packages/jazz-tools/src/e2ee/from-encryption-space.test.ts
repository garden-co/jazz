import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";
import { currentMembershipId } from "./current-membership.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import {
  assertEncryptionPoliciesRegistered,
  usesEncryptionSpacePolicy,
} from "./encrypted-schema.js";

function application() {
  return s.defineApp({
    ...encryptionSpacePolicySchema("projects"),
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
}
it("derives all four operations and keeps membership administration denied by default", () => {
  const app = application();
  const permissions = definePermissions(app, ({ policy }) => policy.notes.fromEncryptionSpace());
  const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);
  const policy = schema.notes!.policies!;
  expect(policy.select!.using).toEqual(policy.insert!.with_check);
  expect(policy.update).toEqual({ using: policy.select!.using, with_check: policy.select!.using });
  expect(policy.delete!.using).toEqual(policy.select!.using);
  expect(JSON.stringify(schema.__e2ee_v1_permission_spaces_projects!.policies)).toContain("Gather");
  for (const table of ["__e2ee_v1_group_members", "__e2ee_v1_space_members_projects"]) {
    expect(schema[table]!.policies!.insert!.with_check).toEqual({ type: "False" });
    expect(schema[table]!.policies!.update!.with_check).toEqual({ type: "False" });
  }
});
it.each(["before", "after"])("rejects explicit policies %s the helper", (order) => {
  const app = application();
  expect(() =>
    definePermissions(app, ({ policy }) => {
      if (order === "before") policy.notes.allowRead.always();
      policy.notes.fromEncryptionSpace();
      if (order === "after") policy.notes.allowInsert.always();
    }),
  ).toThrow(/cannot be combined with explicit policies/);
});
it("rejects missing schema opt-in, ordinary tables and repeated declarations", () => {
  const app = application();
  expect(() =>
    definePermissions(app, ({ policy }) => policy.projects.fromEncryptionSpace()),
  ).toThrow(/requires an encrypted table/);
  expect(() =>
    definePermissions(app, ({ policy }) => {
      policy.notes.fromEncryptionSpace();
      policy.notes.fromEncryptionSpace();
    }),
  ).toThrow(/already specified/);
  const ordinary = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  expect(() =>
    definePermissions(ordinary, ({ policy }) => policy.notes.fromEncryptionSpace()),
  ).toThrow(/encryptionSpacePolicySchema\("projects"\)/);
});
it("requires successful local policy registration and keeps it scoped to the application", () => {
  const app = application();
  const other = application();
  expect(() => assertEncryptionPoliciesRegistered(app.wasmSchema)).toThrow(
    /Import your permissions module/,
  );
  expect(() =>
    definePermissions(app, ({ policy }) => {
      policy.notes.fromEncryptionSpace();
      policy.notes.allowInsert.always();
    }),
  ).toThrow(/cannot be combined/);
  expect(() => assertEncryptionPoliciesRegistered(app.wasmSchema)).toThrow(/not registered/);
  definePermissions(app, ({ policy }) => policy.notes.fromEncryptionSpace());
  expect(usesEncryptionSpacePolicy(app.notes.where({ body: "test" })._schema, "notes")).toBe(true);
  expect(usesEncryptionSpacePolicy(app.wasmSchema, "projects")).toBe(false);
  expect(() => assertEncryptionPoliciesRegistered(other.wasmSchema)).toThrow(/not registered/);
  definePermissions(other, ({ policy }) => policy.notes.allowInsert.always());
  expect(usesEncryptionSpacePolicy(other.wasmSchema, "notes")).toBe(false);
});
it("pins the v1 direct-edge identity and separates namespaces and recipient kinds", () => {
  const parent = "11111111-1111-4111-8111-111111111111";
  const recipient = "22222222-2222-4222-8222-222222222222";
  expect(currentMembershipId("group", parent, "account", recipient)).toBe(
    "ddc6f8e8-9420-8f31-bd5a-acae8c1079e6",
  );
  expect(
    new Set([
      currentMembershipId("group", parent, "account", recipient),
      currentMembershipId("group", parent, "group", recipient),
      currentMembershipId("space", parent, "account", recipient),
    ]).size,
  ).toBe(3);
});
