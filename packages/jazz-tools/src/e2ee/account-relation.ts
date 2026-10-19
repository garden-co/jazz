import { rel, type ForwardRelationship } from "../relationships.js";

/** A relationship to the SDK-managed encryption identity for a Jazz account. */
export function accountRelation(): ForwardRelationship<"__e2ee_account_identities", "accountId">;
export function accountRelation<const Column extends string>(
  column: Column,
): ForwardRelationship<"__e2ee_account_identities", Column>;
export function accountRelation(column = "accountId") {
  return rel("__e2ee_account_identities", column);
}
