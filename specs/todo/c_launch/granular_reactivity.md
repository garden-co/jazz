# Granular Reactivity & Framework Bindings — TODO (Launch)

Design the subscribe/patch system so framework bindings can do fine-grained state updates.

## Problem

Today, query subscriptions emit full result sets. Framework integrations (React, Svelte, Vue, Solid) re-render by diffing the entire array. This works but leaves performance and DX on the table:

- React: `useSyncExternalStore` forces full snapshot comparison. `useReducer` with patches would unlock view transitions, suspense, and concurrent features.
- Svelte: runes/stores work best with granular signals, not wholesale replacement.
- Solid: signals are inherently granular — feeding them full snapshots defeats the purpose.

## Design Direction

The core subscribe system should emit **patches** (row inserted, row updated with changed fields, row removed) rather than full snapshots. Each framework binding translates patches into its native reactivity primitive:

- **React**: `useReducer` that applies patches → supports transitions, suspense, startTransition
- **Svelte**: runes or writable stores updated per-field
- **Solid**: signals per row/field
- **Vue**: reactive refs updated granularly

### Patch shape (strawman)

```ts
type Patch<T> =
  | { op: "insert"; row: T }
  | { op: "update"; id: string; fields: Partial<T> }
  | { op: "delete"; id: string };
```

## Relationship to Existing Work

- `a_week_2026_02_09/minimal_react_bindings.md` ships first with `useSyncExternalStore` — that's fine for MVP
- This spec upgrades the plumbing so bindings can be smarter post-MVP
- The Rust query graph's `Materialize` node already knows which rows changed — the patch info exists, it just isn't surfaced through the subscription API yet

## Open Questions

- Where does patch computation live? Rust (QueryGraph emits patches) vs. TypeScript (diff in the binding)?
- How do patches interact with the worker bridge? (Today: full result sets over postMessage)
- Can we keep the simple `T[]` API as sugar on top of the patch stream?
- Does `useReducer` actually work with external subscription patterns, or does it fight React's ownership model?
- How to handle initial load (full snapshot) vs. subsequent updates (patches)?
