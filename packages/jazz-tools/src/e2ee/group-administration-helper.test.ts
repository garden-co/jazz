import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encryptionSpacePolicySchema } from "./current-membership-schema.js";
import { groupSchema, deviceRequestSchema } from "./managed-schema.js";
import { e2eeAccountRelation } from "./account-relation.js";

function application(currentMembership = true) {
  return s.defineApp({
    ...(currentMembership ? encryptionSpacePolicySchema("projects") : {}),
    projects: s.table({ title: s.string() }, {}),
    // Application names must not collide with SDK administration names.
    e2ee: s.table({ name: s.string() }, {}),
    groups: s.table({ name: s.string() }, {}),
    groupOwners: s.table(
      { groupId: s.uuid(), accountId: s.uuid() },
      { group: s.rel("__e2ee_groups", "groupId"), account: e2eeAccountRelation() },
    ),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
}

it.each([false, true])(
  "compiles owner administration with current membership enabled=%s",
  (current) => {
    const app = application(current);
    const permissions = definePermissions(app, ({ policy: p, e2ee, session }) => {
      p.groups.allowRead.always();
      p.e2ee.allowRead.always();
      e2ee.groups.allowInsert.where({ creatorAccountId: session.user.account });
      e2ee.groups.allowUpdate.where({ creatorAccountId: session.user.account });
    });
    const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);
    const root = schema.__e2ee_groups!.policies!;
    expect(root.insert!.with_check).toMatchObject({ type: "And" });
    expect(JSON.stringify(root.insert)).toContain('"$createdBy.account"');
    expect(JSON.stringify(root)).not.toContain("creatorAccountId");
    expect(root.update!.using).toMatchObject({ type: "And" });
    expect(root.update!.with_check).toEqual({ type: "False" });
    expect(root.delete!.using).toEqual({ type: "False" });
    for (const [table, writer] of [
      ["__e2ee_group_membership", "authorAccountId"],
      ["__e2ee_group_successors", "authorAccountId"],
      ["__e2ee_group_deliveries", "senderAccountId"],
      ["__e2ee_group_recovery_deliveries", "senderAccountId"],
    ] as const) {
      const policy = schema[table]!.policies!;
      expect(JSON.stringify(policy.insert)).toContain(`"${writer}"`);
      expect(policy.insert!.with_check).toMatchObject({
        type: "And",
        exprs: [
          expect.anything(),
          { type: "Inherits", operation: "Update", via_column: "groupId" },
        ],
      });
      expect(policy.update!.with_check).toEqual({ type: "False" });
      expect(policy.delete!.using).toEqual({ type: "False" });
    }
    expect(JSON.stringify(schema.__e2ee_group_repairs!.policies!.insert)).toContain(
      '"$createdBy.account"',
    );
    if (current) {
      const members = schema.__e2ee_v1_group_members!.policies!;
      expect(JSON.stringify(members.insert)).toContain("ExistsIncludingCreated");
      expect(JSON.stringify(members.update)).not.toContain("ExistsIncludingCreated");
      expect(members.update!.using).toEqual(members.update!.with_check);
      expect(members.delete!.using).toEqual({ type: "False" });
    }
  },
);

it("keeps creation and administration denied when their rules are omitted", () => {
  const app = application();
  const permissions = definePermissions(app, ({ e2ee }) => e2ee.groups.allowUpdate.always());
  expect(permissions.__e2ee_groups!.insert!.with_check).toMatchObject({
    type: "And",
    exprs: [expect.anything(), { type: "False" }],
  });
  const createOnly = definePermissions(app, ({ e2ee }) => e2ee.groups.allowInsert.always());
  expect(createOnly.__e2ee_groups!.update!.using).toMatchObject({
    type: "And",
    exprs: [expect.anything(), { type: "False" }],
  });
});

it("rebases callback and compound predicates onto the membership's group reference", () => {
  const app = application();
  const permissions = definePermissions(app, ({ e2ee, policy: p, session, anyOf }) => {
    e2ee.groups.allowInsert.always();
    e2ee.groups.allowUpdate.where((group) =>
      anyOf([
        { creatorAccountId: session.user.account },
        p.groupOwners.existsIncludingCreated.where({
          groupId: group.id,
          accountId: session.user.account,
        }),
      ]),
    );
  });
  const encoded = JSON.stringify(permissions.__e2ee_groups!.update);
  expect(encoded).toContain('"__jazz_outer_row","id"');
  expect(encoded).toContain('"column":"groupId"');
  expect(encoded).not.toContain("creatorAccountId");
  expect(encoded).not.toContain("ExistsIncludingCreated");
  const members = JSON.stringify(permissions.__e2ee_v1_group_members!.insert);
  expect(members).toContain('"table":"groupOwners"');
  expect(members).toContain('"__jazz_outer_row","groupId"');
  expect(members).not.toContain('"__jazz_outer_row","id"');
});

it("keeps negated creator checks inside a positive lookup of the initial group", () => {
  const app = application();
  const permissions = definePermissions(app, ({ e2ee, raw }) => {
    e2ee.groups.allowInsert.always();
    e2ee.groups.allowUpdate.where(
      raw({
        type: "Not",
        expr: {
          type: "Cmp",
          column: "creatorAccountId",
          op: "Eq",
          value: { type: "Literal", value: "11111111-1111-4111-8111-111111111111" },
        },
      }),
    );
  });
  expect(permissions.__e2ee_v1_group_members!.insert!.with_check).toMatchObject({
    type: "And",
    exprs: [
      expect.anything(),
      expect.anything(),
      {
        type: "ExistsIncludingCreated",
        condition: {
          type: "And",
          exprs: [expect.anything(), { type: "Not", expr: { column: "accountId" } }],
        },
      },
    ],
  });
});

for (const table of [...Object.keys(groupSchema), "__e2ee_v1_group_members"]) {
  it.each([false, true])(
    `rejects raw policies for ${table} before or after the helper (%s)`,
    (before) => {
      const app = application();
      expect(() =>
        definePermissions(app, ({ e2ee, policy }) => {
          const raw = () => policy[table as "__e2ee_groups"].allowInsert.always();
          if (before) raw();
          e2ee.groups.allowUpdate.always();
          if (!before) raw();
        }),
      ).toThrow(/Cannot combine group administration helpers with explicit policies/);
    },
  );
}

it("leaves raw policies available when the helper is unused", () => {
  const app = application();
  const permissions = definePermissions(app, ({ policy: p }) =>
    p.__e2ee_groups.allowInsert.always(),
  );
  expect(permissions.__e2ee_groups!.insert!.with_check).toEqual({ type: "True" });
  expect(permissions.__e2ee_group_deliveries).toBeUndefined();
});

it("supports explicitly declared group-only schemas and rejects missing managed tables", () => {
  const app = s.defineApp({ ...deviceRequestSchema, ...groupSchema });
  expect(() =>
    definePermissions(app, ({ e2ee, session }) => {
      e2ee.groups.allowInsert.where({ creatorAccountId: session.user.account });
      e2ee.groups.allowUpdate.where({ creatorAccountId: session.user.account });
    }),
  ).not.toThrow();
  const ordinary = s.defineApp({ projects: s.table({ name: s.string() }, {}) });
  expect(() => definePermissions(ordinary, ({ e2ee }) => e2ee.groups.allowInsert.always())).toThrow(
    /requires the managed group schema/,
  );
});

it("rejects implementation fields and unsupported relational expressions", () => {
  const app = application();
  expect(() =>
    definePermissions(app, ({ e2ee, raw }) => {
      e2ee.groups.allowUpdate.where(
        raw({
          type: "Cmp",
          column: "epochId",
          op: "Eq",
          value: { type: "Literal", value: "forged" },
        }),
      );
    }),
  ).toThrow(/only reference group.id and group.creatorAccountId/);
  expect(() =>
    definePermissions(app, ({ e2ee, raw }) => {
      e2ee.groups.allowUpdate.where(
        raw({ type: "Inherits", operation: "Update", via_column: "accountId" }),
      );
    }),
  ).toThrow(/not supported in group administration callbacks/);
});
