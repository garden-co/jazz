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
      .table(
        { projectId: s.uuid(), body: s.string(), visibility: s.string() },
        { project: s.rel("projects", "projectId") },
      )
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
it.each(["read", "insert", "update", "delete"] as const)(
  "ANDs the optional %s restriction with membership and leaves other operations unchanged",
  (operation) => {
    const app = application();
    const baseline = definePermissions(app, ({ policy: p }) => p.notes.fromEncryptionSpace());
    const permissions = definePermissions(app, ({ policy: p, session }) =>
      p.notes.fromEncryptionSpace({ [operation]: { "$createdBy.account": session.user.account } }),
    );
    const before = mergePermissionsIntoWasmSchema(app.wasmSchema, baseline).notes!.policies!;
    const after = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions).notes!.policies!;
    const clause = { read: "select", insert: "insert", update: "update", delete: "delete" }[
      operation
    ];
    for (const name of ["select", "insert", "update", "delete"] as const) {
      if (name !== clause) expect(after[name]).toEqual(before[name]);
    }
    const restricted =
      operation === "insert"
        ? after.insert!.with_check
        : operation === "read"
          ? after.select!.using
          : after[operation]!.using;
    expect(restricted).toMatchObject({
      type: "And",
      exprs: [before.select!.using, expect.anything()],
    });
    if (operation === "update") expect(after.update!.with_check).toEqual(after.update!.using);
  },
);
it("combines compound reads, common writes and operation-specific callback restrictions", () => {
  const app = application();
  const baseline = definePermissions(app, ({ policy: p }) => p.notes.fromEncryptionSpace());
  const membership = mergePermissionsIntoWasmSchema(app.wasmSchema, baseline).notes!.policies!
    .select!.using;
  const permissions = definePermissions(app, ({ policy: p, session, anyOf }) => {
    p.notes.fromEncryptionSpace({
      read: anyOf([{ "$createdBy.account": session.user.account }, { visibility: "public" }]),
      write: { "$createdBy.account": session.user.account },
      update: (note) => p.projects.exists.where({ id: note.projectId, title: "Editable" }),
      delete: anyOf([]),
    });
  });
  const policy = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions).notes!.policies!;
  expect(policy.select!.using).toMatchObject({ type: "And", exprs: [membership, { type: "Or" }] });
  expect(policy.insert!.with_check).toMatchObject({
    type: "And",
    exprs: [membership, expect.anything()],
  });
  expect(policy.update!.using).toMatchObject({
    type: "And",
    exprs: [membership, expect.anything(), expect.anything()],
  });
  expect(policy.update!.with_check).toEqual(policy.update!.using);
  expect(policy.delete!.using).toMatchObject({
    type: "And",
    exprs: [membership, expect.anything(), { type: "False" }],
  });
});
it("fails closed for invalid extension options and does not register failed policies", () => {
  for (const options of [null, [], { typo: {} }, { read: null }, { update: () => undefined }]) {
    const app = application();
    expect(() =>
      definePermissions(app, ({ policy: p }) =>
        // Untyped callers must not silently lose an intended restriction.
        p.notes.fromEncryptionSpace(options as never),
      ),
    ).toThrow();
    expect(() => assertEncryptionPoliciesRegistered(app.wasmSchema)).toThrow(/not registered/);
  }
});
it("rejects ordinary tables and repeated declarations and infers the membership schema", () => {
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
  ).not.toThrow();
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
