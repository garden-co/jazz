import type { PolicyExpr, TablePolicies } from "../schema.js";
import { currentGroupMembersTable } from "../e2ee/current-membership-names.js";
import { groupSchema } from "../e2ee/managed-schema.js";

export type GroupAdministrationAction = "insert" | "update";
export type GroupAdministrationRules = Record<GroupAdministrationAction, PolicyExpr[]>;

const denied: PolicyExpr = { type: "False" };
const account = { type: "SessionRef" as const, path: ["user", "account"] };
const all = (...exprs: PolicyExpr[]): PolicyExpr => ({ type: "And", exprs });
const any = (exprs: PolicyExpr[]): PolicyExpr =>
  exprs.length === 0 ? denied : exprs.length === 1 ? exprs[0]! : { type: "Or", exprs };
const authenticated: PolicyExpr = all(
  {
    type: "SessionInList",
    path: ["authMode"],
    values: [
      { type: "Literal", value: "local-first" },
      { type: "Literal", value: "external" },
    ],
  },
  { type: "SessionIsNotNull", path: ["user", "account"] },
);
const author = (column: string): PolicyExpr =>
  all(
    authenticated,
    { type: "Cmp", column, op: "Eq", value: account },
    { type: "Cmp", column: "$createdBy.account", op: "Eq", value: account },
  );

function acceptedOnly(expr: PolicyExpr): PolicyExpr {
  if (expr.type === "Exists" || expr.type === "ExistsIncludingCreated")
    return { ...expr, type: "Exists", condition: acceptedOnly(expr.condition) };
  if (expr.type === "And" || expr.type === "Or")
    return { ...expr, exprs: expr.exprs.map(acceptedOnly) };
  if (expr.type === "Not") return { ...expr, expr: acceptedOnly(expr.expr) };
  return expr;
}

/** Public creator naming is a policy facade; signed group records keep their encoding. */
export function groupAdministrationPredicate(expr: PolicyExpr, root = true): PolicyExpr {
  const column = (name: string): string => {
    if (name === "id") return name;
    if (name === "creatorAccountId") return "accountId";
    throw new Error(
      "Group administration callbacks may only reference group.id and group.creatorAccountId",
    );
  };
  const rebase = (value: unknown): unknown => {
    if (!value || typeof value !== "object") return value;
    if (Array.isArray(value)) return value.map(rebase);
    const record = value as Record<string, unknown>;
    if (record.type === "Literal") return value;
    if (
      record.type === "SessionRef" &&
      Array.isArray(record.path) &&
      record.path[0] === "__jazz_outer_row"
    ) {
      if (record.path.length !== 2 || typeof record.path[1] !== "string")
        throw new Error("Unsupported group administration row reference");
      return { ...record, path: ["__jazz_outer_row", column(record.path[1])] };
    }
    return Object.fromEntries(Object.entries(record).map(([key, value]) => [key, rebase(value)]));
  };
  if (expr.type === "ExistsRel" || expr.type === "Inherits" || expr.type === "InheritsReferencing")
    throw new Error(
      "Relational and inheritance expressions are not supported in group administration callbacks yet",
    );
  if (expr.type === "Exists" || expr.type === "ExistsIncludingCreated")
    return { ...expr, condition: groupAdministrationPredicate(expr.condition, false) };
  if (expr.type === "And" || expr.type === "Or")
    return { ...expr, exprs: expr.exprs.map((part) => groupAdministrationPredicate(part, root)) };
  if (expr.type === "Not") return { ...expr, expr: groupAdministrationPredicate(expr.expr, root) };
  const copy = { ...expr };
  if (root && "column" in copy) copy.column = column(copy.column);
  return rebase(copy) as PolicyExpr;
}

/** Keep application correlations on the projection's declared group reference. */
function groupMembershipPredicate(expr: PolicyExpr, negated = false): PolicyExpr {
  if (expr.type === "Not") return groupMembershipPredicate(expr.expr, !negated);
  if (expr.type === "And" || expr.type === "Or")
    return {
      type: negated ? (expr.type === "And" ? "Or" : "And") : expr.type,
      exprs: expr.exprs.map((part) => groupMembershipPredicate(part, negated)),
    };
  const condition: PolicyExpr = negated ? { type: "Not", expr } : expr;
  if ("column" in expr) {
    if (expr.column === "id") {
      const bound: PolicyExpr = { ...expr, column: "groupId" };
      return negated ? { type: "Not", expr: bound } : bound;
    }
    // Keep negation inside the positive lookup, including for a newly created
    // root. NOT EXISTS against accepted-only roots could authorise a new group.
    return {
      type: "ExistsIncludingCreated",
      table: "__e2ee_groups",
      condition: all(groupReference(), condition),
    };
  }
  const rebase = (value: unknown): unknown => {
    if (!value || typeof value !== "object") return value;
    if (Array.isArray(value)) return value.map(rebase);
    const record = value as Record<string, unknown>;
    if (record.type === "Literal") return value;
    if (
      record.type === "SessionRef" &&
      Array.isArray(record.path) &&
      record.path[0] === "__jazz_outer_row"
    ) {
      if (record.path.length !== 2 || record.path[1] !== "id")
        throw new Error(
          "Correlating group.creatorAccountId into another table is not supported in group administration with current membership yet; correlate group.id through a declared group reference instead",
        );
      return { ...record, path: ["__jazz_outer_row", "groupId"] };
    }
    return Object.fromEntries(Object.entries(record).map(([key, value]) => [key, rebase(value)]));
  };
  // Created application rows cannot establish authority through a negative test.
  return rebase(negated ? { type: "Not", expr: acceptedOnly(expr) } : expr) as PolicyExpr;
}

function groupReference(): PolicyExpr {
  return {
    type: "Cmp",
    column: "id",
    op: "Eq",
    value: { type: "SessionRef", path: ["__jazz_outer_row", "groupId"] },
  };
}

/** Generate ordinary policies without changing signed history or membership storage. */
export function compileGroupAdministration(
  rules: GroupAdministrationRules,
  tables: ReadonlySet<string>,
): Record<string, TablePolicies> {
  const immutable = (insert: PolicyExpr): TablePolicies => ({
    select: { using: authenticated },
    insert: { with_check: insert },
    update: { using: denied, with_check: denied },
    delete: { using: denied },
  });
  const administer = any(rules.update);
  const inherits: PolicyExpr = { type: "Inherits", operation: "Update", via_column: "groupId" };
  const result: Record<string, TablePolicies> = {
    __e2ee_groups: {
      ...immutable(all(author("accountId"), any(rules.insert))),
      // USING is inherited by lifecycle records; the signed root cannot change.
      update: { using: all(authenticated, acceptedOnly(administer)), with_check: denied },
    },
  };
  for (const [table, writer] of [
    ["__e2ee_group_membership", "authorAccountId"],
    ["__e2ee_group_successors", "authorAccountId"],
    ["__e2ee_group_deliveries", "senderAccountId"],
    ["__e2ee_group_recovery_deliveries", "senderAccountId"],
  ] as const)
    result[table] = immutable(all(author(writer), inherits));
  // A self-authored repair request confers no administration or key access.
  result.__e2ee_group_repairs = immutable(author("accountId"));
  if (tables.has(currentGroupMembersTable)) {
    const member: PolicyExpr = all(
      authenticated,
      {
        type: "ExistsIncludingCreated",
        table: "__e2ee_groups",
        condition: groupReference(),
      },
      groupMembershipPredicate(administer),
    );
    result[currentGroupMembersTable] = {
      select: { using: authenticated },
      insert: { with_check: member },
      update: { using: all(authenticated, inherits), with_check: all(authenticated, inherits) },
      delete: { using: denied },
    };
  }
  for (const table of Object.keys(groupSchema))
    if (!tables.has(table)) throw new Error("e2ee.groups requires the managed group schema");
  return result;
}
