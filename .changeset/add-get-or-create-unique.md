---
"jazz-tools": patch
---

Add `getOrCreateUnique` method to CoMap, CoList, and CoFeed

This new method provides a "get or create only" semantic - it returns an existing value as-is, and only uses the provided value when creating a new CoValue. Unlike `upsertUnique`, it does NOT update existing values with the provided value.

Example usage:
```typescript
const billingStatus = await BillingStatus.getOrCreateUnique({
  value: { status: "pending" },
  unique: `billing-${user.$jazz.id}`,
  owner: billingGroup,
});
```

Also deprecates `loadUnique` and `upsertUnique` methods in favor of `getOrCreateUnique`.
