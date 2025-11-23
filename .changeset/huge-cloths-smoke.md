---
"jazz-tools": patch
---

Add `useCoValueRef`, `useAccountRef`, `useCoStateAndRef`, and `useAccountAndRef` hooks to allow access to the full CoValue in cases where the data is not needed for rendering.
- Add `useRef` hook to `createCoValueSubscriptionContext` and `createAccountSubscriptionContext`
