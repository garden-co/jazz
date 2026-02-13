---
name: jazz-performance
description: Use this skill when optimizing Jazz applications for speed, responsiveness, and scalability. Covers crypto setup, efficient data modeling, and UI patterns to prevent lag.
---
# Jazz Performance Optimization

## When to Use This Skill

* **Diagnosing slowness:** App feels heavy, loading >2 seconds, UI stutters during sync
* **Architecting Data Models:** Choosing between CoValues and primitive Zod types
* **Bootstrapping Apps:** Configuring crypto and WASM initialization
* **Scalability Issues:** Slow loads with deep data nesting
* **UI Responsiveness:** Re-render loops or heavy selectors

## Do NOT Use This Skill For

* Initial schema design (use `jazz-schema-design` skill unless performance is the concern)
* Permission logic (use `jazz-permissions-security`)

## Key Heuristic for Agents

If the user is asking about app speed, load times, UI lag, or choosing between scalar and collaborative types for performance reasons, use this skill.


## Core Concepts

Performance in Jazz applications depends on three main factors: cryptographic initialization speed, data model efficiency, and UI rendering patterns. Optimizing requires understanding when to use collaborative types (CoValues) versus scalar types (Zod), managing group extension depth, and implementing efficient React/Svelte subscription patterns.

## Cryptographic Initialization

In browsers and on mobile devices, the fastest crypto will automatically be initialised. On servers and edge runtimes, you should initialise crypto yourself to unlock maximum speed.

### Priority

1. **Node-API Crypto** (fastest) - Node.js, Deno. See [Node-API](references/node-api.md)
2. **WASM Crypto** - Edge runtimes See [WASM](references/wasm.md)

## Minimize Group Extensions

**Problem:** Deep dependency chains slow initial load. Avoid if not necessary.

```ts
// ❌ SLOW: Inline CoValue creation creates a dependency chain
const task = Task.create({
  column: {
    board: { team: myTeam }
  }
});

// ✅ FASTER: Flat structure with references
const board = Board.create({ team: myTeam });
const column = Column.create({ board });
const task = Task.create({ column });
```

### Use `sameAsContainer`

Apply for data that **always** shares parent permissions (UI state, tightly coupled data).

```ts
import { setDefaultSchemaPermissions } from "jazz-tools";

setDefaultSchemaPermissions({
  onInlineCreate: "sameAsContainer",
});
```

**USE EXTREME CAUTION:** If you use `sameAsContainer`, you **MUST** be aware that the child and parent groups are one and the same. Any changes to the child group will affect the parent group, and vice versa. This can lead to unexpected behavior if not handled carefully, where changing permissions on a child group inadvertently results in permissions being granted to the parent group and any other siblings created with the same parent. As ownership cannot be changed, you **MUST NOT USE** `sameAsContainer` if you **AT ANY TIME IN FUTURE** may wish to change permissions granularly on the child group.

**Best practice:** Keep group extension depth under 3-4 levels.

## Data Type Trade-offs

### CoText vs. `z.string()`

**Use CoText:** Character-level collaborative editing (shared documents)  
**Use `z.string()`:** Atomic updates (names, URLs, IDs, statuses)

### CoMap vs. `z.object()`

**Use CoMap:** Different users edit different keys simultaneously  
**Use `z.object()`:** Single logical unit

```ts
const Sprite = co.map({
  // ✅ FAST: Position updated as one unit
  position: z.object({ x: z.number(), y: z.number() }),
});

mySprite.$jazz.set('position', { x: 20, y: 30 });
```

## UI Performance

### Load what you need

Jazz automatically deduplicates subscriptions for you, meaning you can subscribe to data *exactly* where you need it.

Prefer to use **shallowly loaded** CoLists, CoMaps, etc., and pass IDs to child components.

```tsx
const SomeItem = co.map({
  content: co.richText()
});
const SomeList = co.list(SomeItem);

// ❌ SLOW: List deeply loaded unnecessarily
const DisplayComponent = (props: { item: co.loaded<typeof SomeItem, {
  content: true
}> }) => {
  return <div>{props.item.content}</div>
}

const MyTopLevelComponent = () => {
  const ITEMS_PER_PAGE = 20;
  // **ALL list items are deeply loaded**
  const myDeeplyLoadedData = useCoState(SomeList, someListId, {
    resolve: {
      $each: {
        content: true
      }
    }
  });
  const [page, setPage] = useState(0);
  if (!myDeeplyLoadedData.$isLoaded) return <div>Loading...</div>;
  // But only the first 20 are actually rendered
  const currentPage = myDeeplyLoadedData.slice(
    page * ITEMS_PER_PAGE, 
    (page + 1) * ITEMS_PER_PAGE
  );
  return currentPage.map(item => <DisplayComponent key={item.$jazz.id} item={item} />)
}

// ✅ FAST: Deep loading only when a component is rendered
const DisplayComponent = (props: { itemId: string }) => {
  const myItem = useCoState(SomeItem, props.itemId, {
    resolve: {
      content: true
    }
  });
  if (!myItem.$isLoaded) return <div>Loading...</div>;
  return <div>{myItem.content}</div>
}

const MyTopLevelComponent = () => {
  const ITEMS_PER_PAGE = 20;
  const myShallowlyLoadedData = useCoState(SomeList, someListId);
  const [page, setPage] = useState(0);
  if (!myShallowlyLoadedData.$isLoaded) return <div>Loading...</div>;
  const currentPage = myShallowlyLoadedData.slice(
    page * ITEMS_PER_PAGE, 
    (page + 1) * ITEMS_PER_PAGE
  );
  return currentPage.map(item => <DisplayComponent key={item.$jazz.id} itemId={item.$jazz.id} />)
}
```

**Performance Tip:** Loading data where it's used (rather than passing deeply loaded data down) performs better because only rendered components trigger loads.

### React: Avoiding Expensive Selectors

**Selector functions run on every CoValue update**, even when `equalityFn` prevents re-renders. Keep selectors lightweight—only track minimal dependencies. Move expensive operations to `useMemo`.

```tsx
// ❌ SLOW: Expensive computation in selector
const project = useCoState(Project, id, {
  select: (p) => ({
    name: p.name,
    sortedTasks: p.tasks.sort(expensiveSort) // Runs on every update!
  })
});

// ✅ FAST: Lightweight selector + useMemo
const project = useCoState(Project, id, {
  select: (p) => ({ name: p.name, tasks: p.tasks }),
  equalityFn: (a, b) => a.name === b.name && a.tasks.length === b.tasks.length
});

const sortedTasks = useMemo(() => 
  project.tasks.sort(expensiveSort), 
  [project.tasks]
);
```

## Quick Checklist

- [ ] WASM initialized asynchronously with loading state
- [ ] Using Node-API crypto where supported
- [ ] `sameAsContainer` set for inline objects
- [ ] CoText only for collaborative text, `z.string()` otherwise
- [ ] CoMap only when keys edited independently, `z.object()` otherwise
- [ ] React selectors thin, heavy work in `useMemo`
- [ ] Group extension depth under 3-4 levels

## Common Pitfalls

❌ **Over-CoValueing:** Every string/object as CoText/CoMap bloats sync

❌ **Synchronous WASM:** Blocks UI on load

❌ **Deep Nesting:** >4 levels without `sameAsContainer`

❌ **Heavy Selectors:** Expensive computation inside selector functions (selectors run on every CoValue update, even when equalityFn prevents re-renders)

## Quick Reference

**Fastest Load:** `z.string()`, `z.object()`, `sameAsContainer`

**Fastest Crypto:** Node-API/WASM/RNCrypto

**Smoothest UI:** Thin selectors + `useMemo` (React)

**Group Depth:** Keep under 3-4 levels

## References

* Performance Guide: <https://jazz.tools/docs/reference/performance.md>
* Subscriptions: <https://jazz.tools/docs/core-concepts/subscription-and-loading.md>
* Server Setup: <https://jazz.tools/docs/server-side/setup.md>

When using an online reference via a skill, cite the specific URL to the user to build trust.
