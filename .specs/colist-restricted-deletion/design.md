# CoList Restricted Deletion - Design

## Overview

This design adds an **opt-in** permissions variant for **CoLists** that allows:

- **writers** (and `writeOnly`) to **append/prepend** items
- only **manager** and **admin** to **remove** items

The feature is implemented by extending the existing `ownedByGroup` ruleset in [`packages/cojson/src/permissions.ts`](../../packages/cojson/src/permissions.ts) with an additional boolean property, and enforcing the restriction inside [`packages/cojson/src/coValues/coList.ts`](../../packages/cojson/src/coValues/coList.ts).

## Problem Statement

Groups embedded in other Groups currently **cannot** use the `writeOnly` role to achieve "append-only" behavior for nested content. When a CoList is owned by such a Group (`ownedByGroup` ruleset), the only way to let members add items is to give them a full write-capable role (typically `writer`), which also allows them to **delete** items.

## Proposed Solution

Extend the `ownedByGroup` ruleset with an opt-in flag:

- `restrictDeletion: true` on a CoList means **deletion ops are only valid when authored by `admin` or `manager`**
- append-like ops remain valid for `writer`/`writeOnly`/`manager`/`admin` as they are today

This is **CoList-specific** (not a general per-operation permission system for all CoValues). For other coValue, the property is just ignored.

## Architecture / Components

### Why not enforce in `permissions.ts`

The existing permission layer ([`packages/cojson/src/permissions.ts`](../../packages/cojson/src/permissions.ts)) validates transactions inside `determineValidTransactions(coValue)`. However, this runs **before** private (encrypted) transactions are decrypted. The processing pipeline in [`packages/cojson/src/coValueCore/coValueCore.ts`](../../packages/cojson/src/coValueCore/coValueCore.ts) `parseNewTransactions` executes:

1. `loadVerifiedTransactionsFromLogs()` — creates `VerifiedTransaction` objects. For private transactions, `changes` is `undefined`.
2. `determineValidTransactions()` — permission checks run here. **`tx.changes` is unavailable for private transactions.**
3. Decryption loop — `decryptTransactionChangesAndMeta()` populates `tx.changes` for valid transactions.

Since all CoList operations default to `privacy: "private"`, `tx.changes` would be `undefined` during step 2 for any remote transaction. This makes it impossible to distinguish insert ops from delete ops at the permission layer.

### Where the restriction is enforced

The restriction is enforced inside `RawCoList._processNewTransactions()` in [`packages/cojson/src/coValues/coList.ts`](../../packages/cojson/src/coValues/coList.ts). At this point:

- Changes are already **decrypted** and available (the method calls `core.getValidSortedTransactions()` which returns `DecryptedTransaction[]`)
- The owning **group is loaded** (a prerequisite for `ownedByGroup` validation to have passed)
- Each change's **op type** (`"app"`, `"pre"`, `"del"`) is known
- The **author** and **madeAt** timestamp are available on each transaction, enabling time-based role resolution via `group.atTime(madeAt).roleOfInternal(author)`

The `ownedByGroup` logic in `permissions.ts` remains unchanged — it continues to validate write access as before. The deletion restriction is an additional content-layer filter applied by `RawCoList` itself.

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

## Deletion Restriction Logic

### Operation classification (CoList)

CoList edit operations are represented as list op payloads in transactions (see [`packages/cojson/src/coValues/coList.ts`](../../packages/cojson/src/coValues/coList.ts)):

- insert-like: `{ op: "app", ... }`, `{ op: "pre", ... }`
- delete-like: `{ op: "del", insertion: OpID }`

`RawCoList.replace()` issues a single transaction containing both:

- an insertion op (`"app"`)
- a deletion op (`"del"`)

With restricted deletion enabled, **any transaction containing a `"del"` change is treated as a deletion attempt**.

### Enforcement inside `_processNewTransactions()`

`RawCoList._processNewTransactions()` iterates over `DecryptedTransaction[]` returned by `core.getValidSortedTransactions()`. Each transaction exposes `{ txID, changes, madeAt, isValid }`.

For a restricted CoList, before processing a valid transaction's changes, the method checks whether the transaction contains any `{ op: "del" }` change. If it does:

1. Derive the author from `txID.sessionID` via `accountOrAgentIDfromSessionID()` (already imported in `coList.ts`)
2. Resolve the author's role at `madeAt` via `this.group.atTime(madeAt).roleOfInternal(author)`
3. If the role is **not** `admin` or `manager` → **skip the entire transaction**

The `isRestricted` boolean is computed once per `_processNewTransactions()` call from the header ruleset, so the role lookup only runs for transactions that actually contain `del` ops.

### Why skip the entire transaction (not just `del` ops)

`replace()` emits a single transaction with both `app` and `del`. Skipping only the `del` would turn a replace into an append — the old item stays **and** the new item is added. Skipping the entire transaction correctly rejects the replace as a whole.

In practice, CoList operations produce transactions that are either all-insert (`append`/`prepend`), all-delete (`delete`), or insert+delete (`replace`). There is no case where a valid transaction mixes unrelated inserts and deletes.

### Why not mark the transaction invalid via `isValid`

The existing `del` processing in `_processNewTransactions` does **not** check `isValid` — it unconditionally pushes to `deletionsByInsertion`. Deletions from invalid transactions are still applied in the current code (invalid transactions are included via `includeInvalidMetaTransactions: true` so that their insertions can be referenced by other transactions). Merely setting `isValid: false` would not prevent the deletion from taking effect. We must skip `del` processing entirely for restricted deletions.

### Pseudocode

```ts
private _processNewTransactions() {
  const transactions = this.core.getValidSortedTransactions({
    ignorePrivateTransactions: false,
    knownTransactions: this.knownTransactions,
    includeInvalidMetaTransactions: true,
  });

  if (transactions.length === 0) return;

  const ruleset = this.core.verified.header.ruleset;
  const isRestricted =
    ruleset.type === "ownedByGroup" && ruleset.restrictDeletion === true;

  // ...existing setup...

  for (const { txID, changes, madeAt, isValid } of transactions) {
    if (this.isFilteredOut(madeAt)) continue;

    // ...existing lastValidTransaction tracking...

    // Restricted deletion check: skip transactions with del ops from non-admin/manager
    if (isValid && isRestricted) {
      const hasDel = changes.some(
        (c) => typeof c === "object" && c !== null && (c as any).op === "del",
      );

      if (hasDel) {
        const author = accountOrAgentIDfromSessionID(txID.sessionID);
        const role = this.group.atTime(madeAt).roleOfInternal(author);

        if (role !== "admin" && role !== "manager") {
          continue; // Skip entire transaction
        }
      }
    }

    // ...existing change processing loop (app/pre/del)...
  }

  // ...existing rebuild logic...
}
```

### Re-validation on group changes

When a group is updated, `resetParsedTransactions()` triggers a content rebuild via `rebuildFromCore()`. This re-runs `_processNewTransactions()` from scratch, re-evaluating the role check for every transaction against the updated group state.

## Scope and Meaning for Other CoValues

### In scope

- CoList only (`header.type === "colist"`)
- Restricting **removal** (list deletion ops)

### Out of scope

- Generalized per-operation permissions for all CoValues (e.g. CoMap `del`, CoPlainText edits, CoStream semantics)

Rationale:

- `ownedByGroup` is used by multiple CoValue types; applying this globally would require a stable, cross-type operation taxonomy.
- CoPlainText and other CRDTs don't map cleanly to "append vs delete" without deeper semantics.

This feature keeps the surface area small while meeting the primary use-case (append-only-by-default lists with moderator removal).

## Backwards Compatibility / Migration

- The new property is **optional** and defaults to the current behavior.
- Existing `ownedByGroup` CoLists are unchanged unless explicitly created with `restrictDeletion: true`.
- No data migration is required.

## Security Considerations

- The restriction is enforced at the **content layer** inside `RawCoList._processNewTransactions()`. Disallowed deletion transactions are silently skipped — their `del` ops (and any co-located `app` ops from `replace()`) have no effect on the list state.
- The check is time-based (role at transaction time via `group.atTime(madeAt)`), consistent with existing permissions enforcement.
- `permissions.ts` is not modified — the existing `ownedByGroup` role-based validation (admin/manager/writer/writeOnly) continues to run as before. The content-layer check is a **supplementary** restriction, not a replacement.
- Relay/sync nodes that don't decrypt transactions will forward restricted-deletion transactions as normal. The enforcement is at the **receiving node** when it builds the CoList content, which is the same trust model used for all permission checks today.

## Testing Strategy

Add targeted tests in `packages/cojson/src/tests/` following patterns in [`packages/cojson/src/tests/permissions.test.ts`](../../packages/cojson/src/tests/permissions.test.ts):

1. **Default behavior unchanged**
   - `ownedByGroup` CoList without the flag: writer can delete
2. **Restricted deletion enabled**
   - writer can append/prepend — items appear in the list
   - writer cannot delete — item remains in the list after delete attempt
   - manager can delete
   - admin can delete
3. **Replace is blocked for writers**
   - `replace()` includes `"del"` and should be fully skipped for writer under restriction (old item stays, new item is not added)
4. **Multi-change transactions**
   - deletion + insertion in same tx from writer: entire transaction skipped
5. **Role changes over time**
   - deletion authored while writer, later promoted to manager: remains skipped (role evaluated at transaction time)
6. **Sync scenario**
   - remote peer sends a writer-deletion transaction (private or trusting); the receiving node's `RawCoList` skips it when building content
7. **Group update triggers re-evaluation**
   - writer makes deletion, then is promoted to manager; after group update triggers rebuild, the deletion takes effect
