import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../testing/index.js";

it("preserves unrelated administration rules when composing encrypted-app policies", () => {
  const app = s.defineApp({
    projects: s.table({ title: s.string() }, {}),
    notes: s
      .table({ projectId: s.uuid(), body: s.string() }, { project: s.rel("projects", "projectId") })
      .encrypted({ space: "projectId", columns: ["body"] }),
  });
  const base = s.definePermissions(app, ({ policy, session }) => {
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
    policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
  });
  const restrictedGrants = s.definePermissions(app, ({ policy, session }) => {
    policy.__e2ee_space_grants.allowInsert.where({
      authorAccountId: session.user.account,
      operation: "add",
    });
  });
  const composed = { ...base, ...restrictedGrants };
  expect(composed.__e2ee_spaces).toEqual(base.__e2ee_spaces);
  expect(composed.__e2ee_groups).toEqual(base.__e2ee_groups);
  expect(composed.__e2ee_space_grants).toEqual(restrictedGrants.__e2ee_space_grants);

  const merged = mergePermissionsIntoWasmSchema(app.wasmSchema, composed);
  const original = mergePermissionsIntoWasmSchema(app.wasmSchema, base);
  expect(merged.__e2ee_spaces!.policies).toEqual(original.__e2ee_spaces!.policies);
  expect(merged.__e2ee_groups!.policies).toEqual(original.__e2ee_groups!.policies);
  expect(merged.__e2ee_space_successors!.policies).toEqual({
    select: { using: { type: "False" } },
    insert: { with_check: { type: "False" } },
    update: { using: { type: "False" }, with_check: { type: "False" } },
    delete: { using: { type: "False" } },
  });
  expect(merged.__e2ee_group_membership!.policies).toEqual({
    select: original.__e2ee_groups!.policies!.select,
    insert: { with_check: { type: "False" } },
    update: { using: { type: "False" }, with_check: { type: "False" } },
    delete: { using: { type: "False" } },
  });
});
