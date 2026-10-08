import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { deviceRequestSchema, deviceRequestPermissions } from "../device-requests.js";
import { spaceSchema } from "../spaces.js";
import { groupSchema } from "../groups.js";

export const app = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"] }),
});
export const policies = definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.always();
  policy.projects.allowInsert.always();
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.always();
  policy.__e2ee_spaces.allowRead.always();
  policy.__e2ee_spaces.allowInsert.where({ accountId: session.user.account });
  policy.__e2ee_space_grants.allowRead.always();
  policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_space_deliveries.allowRead.always();
  policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
});
export const permissions = { ...deviceRequestPermissions, ...policies };

export const indexedApp = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"], indexes: { title: "equality" } }),
});

export const groupApp = s.defineApp({
  ...deviceRequestSchema,
  ...spaceSchema,
  ...groupSchema,
  projects: s.table({ title: s.string() }, {}),
  notes: s
    .table({ projectId: s.uuid(), title: s.string() }, { project: s.rel("projects", "projectId") })
    .encrypted({ space: "projectId", columns: ["title"] }),
});
export const groupPolicies = definePermissions(groupApp, ({ policy, session }) => {
  policy.__e2ee_groups.allowRead.always();
  policy.__e2ee_groups.allowInsert.where({ accountId: session.user.account });
  policy.__e2ee_group_membership.allowRead.always();
  policy.__e2ee_group_membership.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_group_successors.allowRead.always();
  policy.__e2ee_group_successors.allowInsert.where({ authorAccountId: session.user.account });
  policy.__e2ee_group_deliveries.allowRead.always();
  policy.__e2ee_group_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  policy.__e2ee_group_repairs.allowRead.always();
  policy.__e2ee_group_repairs.allowInsert.where({ accountId: session.user.account });
});
