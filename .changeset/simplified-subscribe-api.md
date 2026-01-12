---
"jazz-tools": minor
---

Simplified the subscribe API by removing error callbacks (`onError`, `onUnauthorized`, `onUnavailable`).

The subscription listener now receives a `Settled` value that includes both loaded and error states. Use `$isLoaded` to check if the value is loaded, and `$jazz.loadingState` for details about error states (`"unauthorized"`, `"unavailable"`, or `"deleted"`).

**Migration example:**

```ts
// Before
MyCoMap.subscribe(id, {
  onUnavailable: (value) => console.log("Unavailable"),
  onUnauthorized: (value) => console.log("Unauthorized"),
}, (value) => {
  console.log(value.field);
});

// After
MyCoMap.subscribe(id, (value) => {
  if (!value.$isLoaded) {
    console.log("Error:", value.$jazz.loadingState);
    return;
  }
  console.log(value.field);
});
```
