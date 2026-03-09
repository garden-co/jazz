import { app, type TodoInclude } from "./app.ts";

const noInclude = app.todos.orderBy("id", "asc");
declare const noIncludeRow: typeof noInclude._rowType;

// @ts-expect-error overlapping FK stays scalar without include()
noIncludeRow.owner?.id;
// @ts-expect-error non-overlapping relation key is absent without include()
noIncludeRow.project?.id;

const withOwner = app.todos.include({ owner: true }).orderBy("id", "asc");
declare const withOwnerRow: typeof withOwner._rowType;
withOwnerRow.owner?.id;

const withProject = app.todos.include({ project: true }).orderBy("id", "asc");
declare const withProjectRow: typeof withProject._rowType;
withProjectRow.project?.id;

const forwarded: TodoInclude = {};
const maybeForwarded = app.todos.include(forwarded).orderBy("id", "asc");
declare const maybeForwardedRow: typeof maybeForwarded._rowType;
// @ts-expect-error widened include must preserve scalar possibility for overlapping FKs
maybeForwardedRow.owner?.id;
maybeForwardedRow.project?.id;

const conditional = Math.random() > 0.5 ? { owner: true as const } : {};
const maybeConditional = app.todos.include(conditional).orderBy("id", "asc");
declare const maybeConditionalRow: typeof maybeConditional._rowType;
// @ts-expect-error union include must preserve scalar possibility for overlapping FKs
maybeConditionalRow.owner?.id;

if (typeof maybeForwardedRow.owner !== "string" && maybeForwardedRow.owner) {
  maybeForwardedRow.owner.id;
}
