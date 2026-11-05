import { rel, type ForwardRelationship } from "../relationships.js";

/** A relationship to the SDK-managed encryption identity for a Jazz account.
 * Accepts a stored UUID column. Provenance fields are not supported as
 * relationship columns by the durable catalogue encoding.
 */
export function e2eeAccountRelation(): ForwardRelationship<
  "__e2ee_account_identities",
  "accountId"
>;
export function e2eeAccountRelation<const Column extends string>(
  column: Column,
): ForwardRelationship<"__e2ee_account_identities", Column>;
export function e2eeAccountRelation(column = "accountId") {
  if (column.startsWith("$"))
    throw new Error(
      `Account relationships through "${column}" are not supported yet. Use a stored UUID owner column.`,
    );
  return rel("__e2ee_account_identities", column);
}
