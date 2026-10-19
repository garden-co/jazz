import type { PolicyExpr, TablePolicies } from "../schema.js";
import { spaceAdministrationTable } from "../e2ee/space-administration-schema.js";
import { currentSpaceMembersTable } from "../e2ee/current-membership-names.js";

export type SpaceAdministrationAction = "read" | "insert" | "update";
export type SpaceAdministrationRules = Map<string, Record<SpaceAdministrationAction, PolicyExpr[]>>;

const denied: PolicyExpr = { type: "False" };
const account = { type: "SessionRef" as const, path: ["user", "account"] };
const authenticated: PolicyExpr = {
  type: "And",
  exprs: [
    {
      type: "SessionInList",
      path: ["authMode"],
      values: [
        { type: "Literal", value: "local-first" },
        { type: "Literal", value: "external" },
      ],
    },
    { type: "SessionIsNotNull", path: ["user", "account"] },
  ],
};
const all = (...exprs: PolicyExpr[]): PolicyExpr => ({ type: "And", exprs });
const any = (exprs: PolicyExpr[]): PolicyExpr => (exprs.length ? { type: "Or", exprs } : denied);
const author = (column: string): PolicyExpr =>
  all(
    authenticated,
    { type: "Cmp", column, op: "Eq", value: account },
    { type: "Cmp", column: "$createdBy.account", op: "Eq", value: account },
  );
const outer = (column: string) => ({
  type: "SessionRef" as const,
  path: ["__jazz_outer_row", column],
});

function acceptedOnly(expr: PolicyExpr): PolicyExpr {
  if (expr.type === "Exists" || expr.type === "ExistsIncludingCreated")
    return { ...expr, type: "Exists", condition: acceptedOnly(expr.condition) };
  if (expr.type === "And" || expr.type === "Or")
    return { ...expr, exprs: expr.exprs.map(acceptedOnly) };
  if (expr.type === "Not") return { ...expr, expr: acceptedOnly(expr.expr) };
  return expr;
}

/** A catalogue intrinsic, resolved before policy evaluation; never a provider claim. */
function scopeBinding(scope: string): PolicyExpr {
  return {
    type: "Cmp",
    column: "scopeId",
    op: "Eq",
    value: { type: "SessionRef", path: ["__jazz_table_identity_v1", scope] },
  };
}

// Membership projections have a typed scope reference rather than a root FK.
// Their logical update callback deliberately exposes only the scope identifier.
export function spaceMemberPredicate(expr: PolicyExpr, outerRow = true): PolicyExpr {
  if (expr.type === "ExistsRel")
    throw new Error(
      "Relational expressions are not supported in space administration callbacks yet",
    );
  if (expr.type === "Exists" || expr.type === "ExistsIncludingCreated")
    return { ...expr, condition: spaceMemberPredicate(expr.condition, false) };
  if (expr.type === "And" || expr.type === "Or")
    return { ...expr, exprs: expr.exprs.map((e) => spaceMemberPredicate(e, outerRow)) };
  if (expr.type === "Not") return { ...expr, expr: spaceMemberPredicate(expr.expr, outerRow) };
  if (expr.type === "Inherits" || expr.type === "InheritsReferencing")
    throw new Error("Inheritance is not supported in space administration callbacks yet");
  const copy = { ...expr };
  if (outerRow && "column" in copy) {
    if (copy.column !== "identifier")
      throw new Error("Space membership callbacks may only reference space.identifier");
    copy.column = "scopeId";
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
      if (record.path.length !== 2 || record.path[1] !== "identifier")
        throw new Error("Space membership callbacks may only reference space.identifier");
      return outer("scopeId");
    }
    return Object.fromEntries(Object.entries(record).map(([key, value]) => [key, rebase(value)]));
  };
  return rebase(copy) as PolicyExpr;
}

/** Compile logical operations once per scope, including sibling encrypted tables. */
export function compileSpaceAdministration(
  rules: SpaceAdministrationRules,
  tables: ReadonlySet<string>,
): Record<string, TablePolicies> {
  const result: Record<string, TablePolicies> = {};
  const add = (table: string, operation: "select" | "insert" | "update", condition: PolicyExpr) => {
    const policy = (result[table] ??= {
      select: { using: denied },
      insert: { with_check: denied },
      update: { using: denied, with_check: denied },
      delete: { using: denied },
    });
    const clause = operation === "insert" ? "with_check" : "using";
    const previous = policy[operation]?.[clause];
    const combined = previous && previous.type !== "False" ? any([previous, condition]) : condition;
    policy[operation] =
      operation === "update" ? { using: combined, with_check: denied } : { [clause]: combined };
  };
  for (const [scope, actions] of rules) {
    const bound = all(authenticated, scopeBinding(scope));
    const administration = spaceAdministrationTable(scope);
    const update = any(actions.update);
    // Validate update expressions even when this scope has no projections yet.
    const member = all(authenticated, spaceMemberPredicate(update));
    add("__e2ee_spaces", "select", all(bound, acceptedOnly(any(actions.read))));
    add("__e2ee_spaces", "insert", all(bound, author("accountId"), any(actions.insert)));
    // USING provides an inheritable administration capability. No root mutation
    // can pass WITH CHECK: signed roots remain immutable.
    add(
      "__e2ee_spaces",
      "update",
      all(bound, {
        type: "InheritsReferencing",
        operation: "Update",
        source_table: administration,
        via_column: "rootId",
      }),
    );
    result[administration] = {
      select: { using: authenticated },
      insert: {
        with_check: all(author("accountId"), {
          type: "ExistsIncludingCreated",
          table: "__e2ee_spaces",
          condition: all(
            { type: "Cmp", column: "id", op: "Eq", value: outer("rootId") },
            { type: "Cmp", column: "identifier", op: "Eq", value: outer("identifier") },
            { type: "Cmp", column: "accountId", op: "Eq", value: outer("accountId") },
            scopeBinding(scope),
          ),
        }),
      },
      update: { using: all(authenticated, acceptedOnly(update)), with_check: denied },
      delete: { using: denied },
    };
    const inherits = (operation: "Select" | "Update"): PolicyExpr => ({
      type: "Inherits",
      operation,
      via_column: "spaceId",
    });
    const initial = (grant: boolean): PolicyExpr => ({
      type: "ExistsIncludingCreated",
      table: "__e2ee_spaces",
      condition: all(
        { type: "Cmp", column: "id", op: "Eq", value: outer("spaceId") },
        scopeBinding(scope),
        { type: "Cmp", column: "accountId", op: "Eq", value: account },
        ...(grant
          ? [
              {
                type: "Cmp" as const,
                column: "initialGrantId",
                op: "Eq" as const,
                value: outer("id"),
              },
            ]
          : []),
      ),
    });
    for (const [table, writer] of [
      ["__e2ee_space_grants", "authorAccountId"],
      ["__e2ee_space_deliveries", "senderAccountId"],
      ["__e2ee_space_successors", "authorAccountId"],
      ["__e2ee_space_recovery_deliveries", "senderAccountId"],
    ] as const) {
      if (!tables.has(table)) continue;
      add(table, "select", inherits("Select"));
      const alternatives = [inherits("Update")];
      if (table === "__e2ee_space_grants")
        alternatives.push(
          all(
            {
              type: "Cmp",
              column: "operation",
              op: "Eq",
              value: { type: "Literal", value: "add" },
            },
            initial(true),
          ),
        );
      add(table, "insert", all(author(writer), any(alternatives)));
    }
    const members = currentSpaceMembersTable(scope);
    if (tables.has(members))
      result[members] = {
        insert: { with_check: member },
        update: { using: acceptedOnly(member), with_check: acceptedOnly(member) },
        delete: { using: denied },
      };
  }
  return result;
}
