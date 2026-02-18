# TypeScript Permissions DSL Redesign (permissions.ts) — TODO (MVP)

Move policy authoring out of `schema/current.ts` into a dedicated `schema/permissions.ts` file, with policy expressions written in a query-like TypeScript DSL that is schema-aware and testable.

## Goals

- Keep schema structure and policy logic separate.
- Make policy code read like query code.
- Reuse generated TypeScript client types for table/column safety.
- Encourage policy tests by convention (`schema/permissions.test.ts`).
- Compile to the existing policy AST/runtime (no Rust permission engine rewrite).

## Non-goals

- Changing SQL policy semantics in Groove.
- Introducing a second runtime enforcement path.
- Replacing SQL schema permissions in this phase.

## File Conventions

- `schema/current.ts`: tables, columns, relations, indexes.
- `schema/permissions.ts`: policy definitions for those tables.
- `schema/permissions.test.ts`: generated starter test (single stub + comment guidance).

## Proposed API Shape

```ts
import { app } from "./app";
import { definePermissions } from "jazz-tools/permissions";

export default definePermissions(app, ({ policy, either, both, session }) => [
  policy.todos.allowRead.where((todo) =>
    either({ ownerId: session.userId }).or(
      policy.todoShares.exists.where({
        todoId: todo.id,
        userId: session.userId,
        canRead: true,
      }),
    ),
  ),

  policy.todos.allowInsert.where({ ownerId: session.userId }),

  policy.todos.allowUpdate
    .whereOld(both({ ownerId: session.userId }).and({ archived: false }))
    .whereNew({ ownerId: session.userId }), // prevent owner reassignment

  policy.todos.allowDelete.where({ ownerId: session.userId }),
]);
```

## Expression Rules

`where(...)`, `whereOld(...)`, and `whereNew(...)` accept either:

- A normal query-style filter object.
- A callback that receives row context and returns a condition.

`either(...)` and `both(...)` create composable condition builders:

- `either(a).or(b).or(c)` => OR chain.
- `both(a).and(b).and(c)` => AND chain.

`policy.<table>.exists.where(...)` expresses existence checks against related tables.

`session.*` resolves JWT claims in policy expressions (`session.userId` maps to claim key configured by auth mapping; default: `sub`).

## Update Semantics

- `allowUpdate.whereOld(...)` compiles to `USING`.
- `allowUpdate.whereNew(...)` compiles to `WITH CHECK`.
- If `whereNew` is omitted, default behavior matches current semantics (fallback to old/update USING rule).

## Compilation Strategy

Keep backend runtime unchanged by compiling new TS DSL to existing `PolicyExpr` AST:

1. Parse permission builder calls into an intermediate TS policy IR.
2. Normalize IR into existing schema policy shape (`select/insert/update/delete` with `using`/`with_check`).
3. Reuse existing SQL generation and runtime policy evaluation.

## Build Pipeline Changes

Need multi-phase build to avoid circular dependency:

1. Load `schema/current.ts`.
2. Generate temporary typed app client artifact for permissions authoring.
3. Load/compile `schema/permissions.ts`.
4. Emit final schema artifacts and client.
5. If missing, generate `schema/permissions.test.ts` stub once (do not overwrite user edits).

## Generated Client / Typegen Lift

Extend generated typings for policy contexts:

- Row callback parameter typing per table (`todo.id`, etc.).
- `session` claim references as typed placeholders.
- `exists` condition typing across tables and refs.
- Shared filter-object type between query `.where` and policy `.where`.

## Testing Helpers (new package surface)

Target helper API:

```ts
import { createPolicyTestApp } from "jazz-tools/testing";

const t = await createPolicyTestApp({ schema, permissions });
await t.seed(({ db }) => db.todos.insert({ id: "t1", ownerId: "u1" }));

const alice = t.as({ sub: "u1" });
await t.expectAllowed(() => alice.query(t.app.todos).all());
await t.expectDenied(() => alice.mutate(t.app.todos).delete({ id: "t2" }));
```

Helper responsibilities:

- Spin up disposable local Jazz server/runtime.
- Seed synthetic data.
- Mint request-scoped clients with custom claims.
- Assert allow/deny for queries and mutations.

## Rollout Plan

1. Add new DSL package surface (`jazz-tools/permissions`) with typed builders and IR.
2. Add compiler from new DSL IR to existing `PolicyExpr`.
3. Add CLI support for `schema/permissions.ts`.
4. Add `permissions.test.ts` stub generation + testing helpers.
5. Migrate docs examples and `examples/docs/*` to new pattern.
6. Add compatibility path:
   - Continue supporting inline `permissions` in `current.ts` during transition.
   - If both are present, fail with clear error.

## Open Questions

- Session claim mapping: global config only, or per-policy override?
- Whether callback `where((row) => ...)` should allow arbitrary logic or only condition-builder returns.
- How far to support nested include-style predicates in MVP.
- Exact error model for `expectDenied` assertions (status code vs structured reason).

## Success Criteria

- New example app policies read like query logic and typecheck.
- Existing SQL/runtime permission behavior remains unchanged.
- Policy tests can be authored with <20 lines for common allow/deny cases.
- Docs can teach permissions without exposing hashed migration filenames or snippet artifacts.
