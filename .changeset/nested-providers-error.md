---
"jazz-tools": minor
---

Removed `JazzContextManagerContext` and added error when nesting `JazzProvider` components. This prevents bad patterns like nested providers and simplifies the alternative approach of using `JazzContext.Provider` directly with `useJazzContext()`.

### Breaking changes

- Removed `JazzContextManagerContext` export from `jazz-tools/react-core`
- Renamed `useJazzContext` to `useJazzContextValue` (returns the context value)
- `useJazzContext` now returns the context manager instead of the context value
- Nesting `JazzProvider` components now throws an error

### Migration

If you were using `useJazzContext` to get the context value, rename it to `useJazzContextValue`:

```diff
- import { useJazzContext } from "jazz-tools/react-core";
+ import { useJazzContextValue } from "jazz-tools/react-core";

- const context = useJazzContext();
+ const context = useJazzContextValue();
```

If you need to provide context to children without creating a new context (e.g., for components that don't propagate React context), use:

```tsx
<JazzContext.Provider value={useJazzContext()}>
  {children}
</JazzContext.Provider>
```

