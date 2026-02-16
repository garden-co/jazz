# CoList Restricted Deletion - Design

## Overview

This design adds an **opt-in** permissions variant for **CoLists** that allows:

- **writers** (and `writeOnly`) to **append/prepend** items
- only **manager** and **admin** to **remove** items

The feature is implemented by extending the existing `ownedByGroup` ruleset in [`packages/cojson/src/permissions.ts`](../../packages/cojson/src/permissions.ts) with an additional boolean property.

## Problem Statement

Groups embedded in other Groups currently **cannot** use the `writeOnly` role to achieve “append-only” behavior for nested content. When a CoList is owned by such a Group (`ownedByGroup` ruleset), the only way to let members add items is to give them a full write-capable role (typically `writer`), which also allows them to **delete** items.

## Proposed Solution

Extend the `ownedByGroup` ruleset with an opt-in flag:

- `restrictDeletion: true` on a CoList means **deletion ops are only valid when authored by `admin` or `manager`**
- append-like ops remain valid for `writer`/`writeOnly`/`manager`/`admin` as they are today

This is **CoList-specific** (not a general per-operation permission system for all CoValues). For other coValue, the property is just ignored. 

## Architecture / Components

### Where permissions are enforced

Owned CoValues are validated in [`packages/cojson/src/permissions.ts`](../../packages/cojson/src/permissions.ts) inside `determineValidTransactions(coValue)`.

For `ownedByGroup`, the current logic:

1. loads the owning group
2. computes the author’s effective role at the transaction’s time
3. marks the transaction valid for any role in `{ admin, manager, writer, writeOnly }`

### How to scope the restriction to CoList

The verified header includes the CoValue type:

- [`packages/cojson/src/coValueCore/verifiedState.ts`](../../packages/cojson/src/coValueCore/verifiedState.ts) defines:
  - `CoValueHeader.type: AnyRawCoValue["type"]`

So validation can safely gate the restriction with:

- `coValue.verified.header.type === "colist"`

This avoids changing semantics for other CoValue types that also use `ownedByGroup`.

## Data Models

### Ruleset type change

Extend `ownedByGroup` in [`packages/cojson/src/permissions.ts`](../../packages/cojson/src/permissions.ts):

```ts
export type PermissionsDef =
  | { type: "group"; initialAdmin: RawAccountID | AgentID }
  | { type: "ownedByGroup"; group: RawCoID; restrictDeletion?: boolean }
  | { type: "unsafeAllowAll" };
```

Notes:

- The flag is optional for compatibility.
- The behavior change is **opt-in**: only when `restrictDeletion === true`.

## Permission Validation

### Operation classification (CoList)

CoList edit operations are represented as list op payloads in transactions (see [`packages/cojson/src/coValues/coList.ts`](../../packages/cojson/src/coValues/coList.ts)):

- insert-like: `{ op: "app", ... }`, `{ op: "pre", ... }`
- delete-like: `{ op: "del", insertion: OpID }`

`RawCoList.replace()` issues a single transaction containing both:

- an insertion op (`"app"`)
- a deletion op (`"del"`)

With restricted deletion enabled, **any transaction containing a `"del"` change is treated as a deletion attempt**.

### Validation rule

For CoValues with:

- `ruleset.type === "ownedByGroup"`
- `ruleset.restrictDeletion === true`
- `header.type === "colist"`

then for each transaction:

- if any change has `{ op: "del" }`:
  - require role \(\in \{ "admin", "manager" \}\)
  - otherwise mark invalid (e.g. `"Deletion is restricted to admins/managers"`)

All other owned-by-group validation remains unchanged, including:

- branch-pointer meta special-casing for `reader` (the existing `{ meta: { branch, ownerId } }` path)
- time-based role resolution (role is evaluated at `tx.currentMadeAt`)

### Pseudocode

```ts
if (coValue.verified.header.ruleset.type === "ownedByGroup") {
  // ...existing group lookup and role resolution...

  const ruleset = coValue.verified.header.ruleset;
  const isRestrictedCoList =
    ruleset.restrictDeletion === true && coValue.verified.header.type === "colist";

  for (const tx of coValue.toValidateTransactions) {
    // ...existing checks...

    const role = groupAtTime.roleOfInternal(effectiveTransactor);
    if (!isWriteCapable(role)) markInvalid();

    if (isRestrictedCoList && tx.changes.some((c) => isObject(c) && c.op === "del")) {
      if (role !== "admin" && role !== "manager") markInvalid();
    }

    markValid();
  }
}
```

## Scope and Meaning for Other CoValues

### In scope

- CoList only (`header.type === "colist"`)
- Restricting **removal** (list deletion ops)

### Out of scope

- Generalized per-operation permissions for all CoValues (e.g. CoMap `del`, CoPlainText edits, CoStream semantics)

Rationale:

- `ownedByGroup` is used by multiple CoValue types; applying this globally would require a stable, cross-type operation taxonomy.
- CoPlainText and other CRDTs don’t map cleanly to “append vs delete” without deeper semantics.

This feature keeps the surface area small while meeting the primary use-case (append-only-by-default lists with moderator removal).

## Backwards Compatibility / Migration

- The new property is **optional** and defaults to the current behavior.
- Existing `ownedByGroup` CoLists are unchanged unless explicitly created with `restrictDeletion: true`.
- No data migration is required.

## Security Considerations

- The restriction is enforced during **transaction validation**, so disallowed deletions become **invalid transactions** and are ignored by readers.
- The check is time-based (role at transaction time), consistent with existing permissions enforcement.

## Testing Strategy

Add targeted tests in `packages/cojson/src/tests/` following patterns in [`packages/cojson/src/tests/permissions.test.ts`](../../packages/cojson/src/tests/permissions.test.ts):

1. **Default behavior unchanged**
   - `ownedByGroup` CoList without the flag: writer can delete
2. **Restricted deletion enabled**
   - writer can append/prepend
   - writer cannot delete
   - manager can delete
   - admin can delete
3. **Replace is blocked for writers**
   - `replace()` includes `"del"` and should be invalid for writer under restriction
4. **Multi-change transactions**
   - deletion + insertion in same tx: restricted by deletion rule
5. **Role changes over time**
   - deletion authored while writer, later promoted: remains invalid
