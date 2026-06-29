# `unchanged(...)` helper for column-immutability in update policies

## What

Update policies that forbid specific columns from changing currently rely on a
cryptic `exists` self-join. From the chat example (`examples/chat-react/permissions.ts`):

```ts
policy.chats.allowUpdate.whereOld(userIsChatMember).whereNew((chat) =>
  allOf([
    userIsChatMember(chat),
    // createdBy and isPublic must not change
    policy.chats.exists.where({
      id: chat.id,
      createdBy: chat.createdBy,
      isPublic: chat.isPublic,
    }),
  ]),
);
```

It reads as "a chat with this id and these values exists", not "these columns are
immutable" — the intent is buried in a self-correlated existence check. Propose a
helper that states it directly:

```ts
policy.chats.allowUpdate
  .whereOld(userIsChatMember)
  .whereNew((chat) =>
    allOf([userIsChatMember(chat), policy.chats.unchanged("createdBy", "isPublic")]),
  );
```

## Why the current trick works (and why it's opaque)

Inside `whereNew`, `chat.createdBy` is a row-ref marker that compiles to
`@__jazz_outer_row.createdBy` — the submitted (new) value — while `exists` queries
stored state. The row matches only when stored(old) == submitted(new), i.e. the
column did not change; the `id` term self-correlates to the same row. Trail:
`packages/jazz-tools/src/permissions/index.ts` (`createRowContext`, `compileCondition`
→ `Exists`, `toPolicyValue` → `OUTER_ROW_SESSION_PREFIX`) and
`crates/jazz-tools/src/query_manager/policy.rs` (`OUTER_ROW_SESSION_PREFIX`).

## API shape

Pure TypeScript sugar over the existing `Exists` IR node — no new `PolicyExpr`
variant and no evaluator change. `policy.<table>.unchanged(...cols)` emits exactly
what the manual pattern does:

```ts
{
  __jazzPermissionKind: "exists",
  table: "<table>",
  where: { id: rowRef("id"), [col]: rowRef(col), ... },
}
```

Because the row-refs are static column-name markers (values are injected at eval
time via `__jazz_outer_row`), the helper does not need the runtime row object.

Two call-site options:

- **A (recommended) — method on the table builder:**
  `policy.chats.unchanged("createdBy", "isPublic")`. The table is already in scope
  via `policy.chats`; columns type as `keyof Row` (autocompleted, typo-caught). Sits
  beside the existing `policy.<table>.exists`. Zero internal changes beyond the new
  method.
- **B — free function in the builder object:**
  `unchanged(chat, ["createdBy", "isPublic"])`, exposed alongside `allOf`/`anyOf` in
  the `definePermissions` context (constructed in `permissions/index.ts`). Reads
  closest to a bare sketch and avoids repeating the table name, but requires tagging
  the `RowContext` proxy with a hidden table symbol so the helper can resolve the
  table.

## Notes / caveats

- **Update-only.** "Unchanged" is meaningful only in a `whereNew` context; it is
  nonsensical for insert/read/delete. Constrain the type so it is offered only there
  (or at minimum document it), otherwise it silently desugars to an `exists` that
  behaves oddly outside updates.
- **Semantics = "equal to the currently-stored value."** For a normal update that is
  the pre-update value, which is the intent — but it is an `exists`-against-stored-state,
  not a literal old/new diff. The doc comment should say so.
- **Migration path.** There is no `OldEqualsNew` / `CompareRowColumns` primitive in the
  IR (`exists` is the only old-vs-new mechanism today). If one is ever added, `unchanged`
  can re-point to it with no user-code change.
- **Out of scope: ownership-via-FK.** This does not tidy the separate pattern
  `policy.profiles.exists.where({ id: message.senderId, userId: session.user_id })`
  (message-delete ownership) — that is column-equality-to-a-ref, a different shape.
  `allowedTo.update("<fk>")` already expresses it tersely; its natural sugar would be a
  distinct `allowedTo.referencing("<fk>").matches({ ... })` helper.

Surfaced while fixing the chat-react example delete policy, once PR #980 made
`exists`-on-`id` evaluate correctly.
