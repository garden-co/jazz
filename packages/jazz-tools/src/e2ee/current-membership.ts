import { sha256 } from "@noble/hashes/sha2.js";
import type { WasmSchema } from "../drivers/types.js";
import type { E2eeTransactionScope } from "../runtime/db.js";
import { TypedTableQueryBuilder } from "../typed-app.js";
import {
  currentGroupMembersTable,
  currentSpaceMembersTable,
  permissionSpacesTable,
} from "./current-membership-names.js";

/** Stable v1 direct-edge identity. Revocation clears the indexed endpoints. */
export function currentMembershipId(
  namespace: string,
  parent: string,
  kind: "account" | "group",
  recipient: string,
): string {
  const digest = sha256(
    new TextEncoder().encode(
      JSON.stringify(["jazz.e2ee.policy-membership.v1", namespace, parent, kind, recipient]),
    ),
  ).slice(0, 16);
  digest[6] = (digest[6]! & 0x0f) | 0x80;
  digest[8] = (digest[8]! & 0x3f) | 0x80;
  const hex = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export function maintainGroupMember(
  tx: E2eeTransactionScope,
  schema: WasmSchema,
  groupId: string,
  kind: "account" | "group",
  recipient: string,
  operation: "add" | "remove",
): void {
  if (!schema[currentGroupMembersTable]) return;
  tx.upsert(
    new TypedTableQueryBuilder(currentGroupMembersTable, schema),
    currentMembershipId("group", groupId, kind, recipient),
    {
      groupId,
      recipientId: recipient,
      accountId: operation === "add" && kind === "account" ? recipient : null,
      childGroupId: operation === "add" && kind === "group" ? recipient : null,
    },
  );
}
export function maintainSpaceMember(
  tx: E2eeTransactionScope,
  schema: WasmSchema,
  scope: string,
  scopeIdentity: string,
  identifier: string,
  kind: "account" | "group",
  recipient: string,
  operation: "add" | "remove",
): void {
  const table = currentSpaceMembersTable(scope);
  if (!schema[table]) return;
  tx.upsert(
    new TypedTableQueryBuilder(table, schema),
    currentMembershipId(scopeIdentity, identifier, kind, recipient),
    {
      scopeId: identifier,
      accessId: identifier,
      accountId: operation === "add" && kind === "account" ? recipient : null,
      groupId: operation === "add" && kind === "group" ? recipient : null,
    },
  );
}
export function initialisePermissionSpace(
  tx: E2eeTransactionScope,
  schema: WasmSchema,
  scope: string,
  identifier: string,
): void {
  const table = permissionSpacesTable(scope);
  if (schema[table])
    tx.upsert(new TypedTableQueryBuilder(table, schema), identifier, { scopeId: identifier });
}
