import { rel, type ForwardRelationship } from "../relationships.js";

/** A relationship to the SDK-managed encryption identity for a Jazz account.
 * Accepts a stored UUID column or `$createdBy.account` / `$updatedBy.account`.
 */
export function e2eeAccountRelation(): ForwardRelationship<
  "__e2ee_account_identities",
  "accountId"
>;
export function e2eeAccountRelation<const Column extends string>(
  column: Column,
): ForwardRelationship<"__e2ee_account_identities", Column>;
export function e2eeAccountRelation(column = "accountId") {
  return rel("__e2ee_account_identities", column);
}
