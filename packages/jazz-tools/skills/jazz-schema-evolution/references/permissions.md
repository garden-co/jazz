# Permission policies

Permissions are server-enforced row policies authored in `permissions.ts`. Reads that fail policy
checks are filtered out; writes that fail are rejected.

An app that has never published a permissions bundle has no row-policy gates: reads and writes are
allowed from all actors. Publishing the first bundle switches the app to enforcement. From then on,
every operation requires an explicit grant; define read, insert, update, and delete separately. This
is distinct from a server waiting for an already-published bundle to arrive, which fails closed.

The current permissions bundle also governs historical rows when they are viewed through the current
schema and migration lenses. A later policy can therefore expose or hide data created under an older
policy.

## Contents

- [Creator-managed rows](#creator-managed-rows)
- [Explicit owner columns](#explicit-owner-columns)
- [Unconditional and explicit no-grant rules](#unconditional-and-explicit-no-grant-rules)
- [Relation-aware access](#relation-aware-access)
- [Share-table access](#share-table-access)
- [Session claims](#session-claims)
- [Permission-aware UI](#permission-aware-ui)

## Creator-managed rows

Default to built-in creator metadata when authorship and ownership are the same:

```ts
import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy }) => {
  policy.todos.managedByCreator();
});
```

`managedByCreator()` grants read, insert, update, and delete when `$createdBy` matches the current
session. Use the `isCreator` helper when creator access is only one part of a more specific rule:

```ts
export default s.definePermissions(app, ({ policy, anyOf, isCreator }) => {
  policy.todos.allowRead.where(anyOf([isCreator, { published: true }]));
});
```

## Explicit owner columns

Use an owner column when ownership can be assigned, transferred, or differ from authorship:

```ts
export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user_id });
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});
```

Checking both old and new rows prevents an allowed owner from transferring a row to another owner
unless the policy explicitly permits it. If only `whereOld` or only `whereNew` is supplied, Jazz
applies that condition to both sides; use both when the conditions differ.

## Unconditional and explicit no-grant rules

```ts
policy.posts.allowRead.always();
policy.posts.allowInsert.never();
```

Use `.always()` only for intentionally public operations. Use `.never()` to document that an
operation has no grant. Grants for the same table operation are OR-combined and source order does not
matter: `.never()` does not override another grant, while `.always()` makes the operation
unconditional.

## Relation-aware access

```ts
export default s.definePermissions(app, ({ policy, allowedTo, allOf }) => {
  policy.todos.allowRead.where(allowedTo.read("project"));
  policy.todos.allowUpdate
    .whereOld(allOf([allowedTo.update("project"), { done: false }]))
    .whereNew(allowedTo.update("project"));
});
```

Use `allowedTo.read`, `allowedTo.insert`, `allowedTo.update`, or `allowedTo.delete` to inherit access
from a typed relation. For self-referential relations, pass a bounded positive `maxDepth` when
recursive inheritance is intended.

## Share-table access

```ts
export default s.definePermissions(app, ({ policy, anyOf, session }) => {
  policy.todos.allowRead.where((todo) =>
    anyOf([
      { owner_id: session.user_id },
      policy.todoShares.exists.where({
        todoId: todo.id,
        user_id: session.user_id,
        can_read: true,
      }),
    ]),
  );
});
```

Use `policy.<table>.exists.where(...)` to correlate the current row with membership or sharing rows.

## Session claims

```ts
policy.todos.allowRead.where(
  anyOf([{ owner_id: session.user_id }, session.where({ "claims.role": "manager" })]),
);
```

Match claim names exactly as they appear in the authenticated session. Do not trust unsigned client
state as a claim substitute.

## Permission-aware UI

Select `$canEdit` and `$canDelete` when UI controls need the server policy result. These values are
for affordances, not enforcement; the write must still pass the policy.

Test policies with the `jazz-testing` skill, including denied reads, denied writes, ownership
transfer, and claim-based access.
