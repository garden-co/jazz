import { schemaColumns as s } from "../dsl.js";
import { defineTable, DefinedTable } from "../table-definition.js";
import { rel } from "../relationships.js";
import { assertSchemaNameAllowed } from "../schema-name.js";
import { deviceRequestSchema, groupSchema, spaceSchema } from "./managed-schema.js";
import {
  currentGroupMembersTable,
  currentSpaceMembersTable,
  permissionSpacesPrefix,
  permissionSpacesTable,
} from "./current-membership-names.js";

const groupMembers = defineTable(
  {
    groupId: s.uuid(),
    recipientId: s.uuid(),
    accountId: s.uuid().optional(),
    childGroupId: s.uuid().optional(),
  },
  {
    group: rel("__e2ee_groups", "groupId"),
    account: rel("__e2ee_account_identities", "accountId"),
    childGroup: rel("__e2ee_groups", "childGroupId"),
  },
);
function spaceMembers<const Scope extends string>(scope: Scope) {
  return new DefinedTable(
    {
      scopeId: s.uuid(),
      accessId: s.uuid(),
      accountId: s.uuid().optional(),
      groupId: s.uuid().optional(),
    },
    {
      scope: rel(scope, "scopeId"),
      access: rel(`${permissionSpacesPrefix}${scope}` as const, "accessId"),
      account: rel("__e2ee_account_identities", "accountId"),
      group: rel("__e2ee_groups", "groupId"),
    },
  );
}
function permissionSpaces<const Scope extends string>(scope: Scope) {
  return new DefinedTable({ scopeId: s.uuid() }, { scope: rel(scope, "scopeId") });
}
/** Opt in to current membership maintenance. Applications authorise membership administration separately. */
export function encryptionSpacePolicySchema<const Scope extends string>(scope: Scope) {
  assertSchemaNameAllowed(scope);
  return {
    ...deviceRequestSchema,
    ...groupSchema,
    ...spaceSchema,
    [currentGroupMembersTable]: groupMembers,
    [currentSpaceMembersTable(scope)]: spaceMembers(scope),
    [permissionSpacesTable(scope)]: permissionSpaces(scope),
  } as typeof deviceRequestSchema &
    typeof groupSchema &
    typeof spaceSchema & { __e2ee_v1_group_members: typeof groupMembers } & {
      [Key in `__e2ee_v1_space_members_${Scope}`]: ReturnType<typeof spaceMembers<Scope>>;
    } & {
      [Key in `__e2ee_v1_permission_spaces_${Scope}`]: ReturnType<typeof permissionSpaces<Scope>>;
    };
}
