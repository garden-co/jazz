---
name: jazz-ui-development
description: Use this skill when building, debugging, or optimizing Jazz applications. It covers Jazz's bindings with various different UI frameworks, as well as how to use Jazz without a framework. Look here for details on providers and context, hooks and reactive data fetching, authentication, and specialized UI components for media and inspection.
---

# Jazz UI Development

## When to Use This Skill

*   Building user interfaces that display or interact with Jazz data
*   Setting up authentication providers and contexts
*   Debugging reactivity, data loading, or sync issues in the UI
*   Optimizing performance of data subscriptions
*   Handling media (images, files) in the UI

## Do NOT Use This Skill For

*   Defining data schemas (use `jazz-schema-design`)
*   Complex permission logic or sharing workflows (use `jazz-permissions-security`)
*   Testing questions (use `jazz-testing`)

## Key Heuristic for Agents

Use this skill when the user asks about rendering data, handling user input, managing authentication state, or framework-specific integration (React, Svelte, etc.). If the issue is "data not appearing" but permissions are correct, look here for loading and subscription patterns.


## Framework Guides

Select the guide for your specific framework:

*   **React**: [React Reference](references/react.md)
  *   **React Native (Bare)**: As well as the React guide, also see [React Native Reference](references/react-native.md)
  *   **React Native (Expo)**: As well as the React guide, also see[React Native (Expo) Reference](references/expo.md)
*   **Svelte**: [Svelte Reference](references/svelte.md)
*   **Vanilla JS**: [Vanilla JS Reference](references/vanilla.md)

## Universal CoValue API

These APIs are available on all CoValue instances, regardless of the framework.

### Core Properties

All CoValues (CoMaps, CoLists, etc.) have these properties directly on the instance:

*   `$isLoaded` (`boolean`): Returns `true` if the CoValue is fully loaded and ready to read.
*   `.toJSON()`: Returns a plain JavaScript object representation of the CoValue. **Use this for debugging only**, not for rendering.

### The `.$jazz` Namespace

Every CoValue instance has a `.$jazz` property containing metadata and utility methods.

#### Metadata

*   `.$jazz.id` (`ID<CoValue>`): The globally unique ID of the CoValue (e.g., `co_zXy...`).
*   `.$jazz.owner` (`Group`): The Group that owns this CoValue. Access control is determined by membership in this group.
*   `.$jazz.createdAt` (`number`): Creation timestamp (ms since epoch).
*   `.$jazz.createdBy` (`ID<Account> | undefined`): ID of the creating account.
*   `.$jazz.lastUpdatedAt` (`number`): Last update timestamp (ms since epoch).

#### Loading & Inspection

*   `.$jazz.loadingState` (`"loading" | "loaded" | "unavailable" | "unauthorized"`): The current loading state.
*   `.$jazz.ensureLoaded({ resolve: { ... } })`: Waits for nested references to load. Returns a promise with a **new** instance where requested fields are guaranteed available.

```ts
const detailedPost = await post.$jazz.ensureLoaded({
  resolve: {
    comments: {
      $each: {
        author: true 
      }
    }
  }
});
```

*   `.$jazz.refs` (CoMap/CoRecord only): Access nested CoValues as references (with IDs) without loading them.
*   `.$jazz.has(key)` (CoMap/CoRecord only): Checks if a key exists.

#### Subscription & Sync

*   `.$jazz.subscribe(callback)`: Manually subscribe to changes. Returns an unsubscribe function. **Prefer framework-specific hooks (e.g., `useCoState`) over this.**
*   `.$jazz.waitForSync()`: Promise that resolves when changes are synced to peers. Useful for critical saves or logout flows.

#### Version Control

*   `.$jazz.isBranched` (`boolean`): Whether the CoValue is currently in a local branch.
*   `.$jazz.branchName` (`string`): The name of the current branch.
*   `.$jazz.unstable_merge()`: Merges a branch back into the main history.

## Working with Data Types

### CoMap / CoRecord
*   **Read**: Access properties like a standard JS object (e.g., `user.name`).
*   **Write**:
    *   `.$jazz.set(key, value)`
    *   `.$jazz.delete(key)`
    *   `.$jazz.applyDiff(diff)`

### CoList
*   **Read**: Access by index (`list[0]`), iterate (`map`, `forEach`).
*   **Write**:
    *   `.$jazz.push(item)`
    *   `.$jazz.unshift(item)`
    *   `.$jazz.splice(...)`
    *   `.$jazz.remove(index or predicate)`

### CoText (CoPlainText / CoRichText)
*   **Read**: `toString()` or use directly in template.
*   **Write**:
    *   `insertAfter(index, text)`
    *   `deleteRange({ from, to })`
    *   `.$jazz.applyDiff(newText)`

### CoFeed
*   **Read**: Iterate over `feed.all` (returns per-session feeds).
*   **Write**: `.$jazz.push(item)` (Append-only).

```ts
// Subscribing to all entries across all accounts
const allEntries = Object.values(feed.perAccount).flatMap(accountFeed => Array.from(accountFeed.all));

// Pushing a new entry
feed.$jazz.push({ message: "Hello", timestamp: Date.now() });
```

### DiscriminatedUnion
*   **Read**: Check the discriminator property (e.g. `type`) to narrow the type.
*   **Write**: Overwrite the property with a new object of the desired variant.

```ts
const list = co.list(MyUnion);
const item = list[0];

if (item.type === "text") {
  console.log(item.value); // item is narrowed to TextVariant
} else if (item.type === "image") {
  console.log(item.image.$jazz.id); // item is narrowed to ImageVariant
}
```

### FileStream
*   **Read**: Use `toBlob()` or consume chunks.
*   **Write**: Use `createFromBlob(blobOrFile)` or `start(metadata) -> push(chunk) -> end()`.

### ImageDefinition
*   **Read**: Use `<Image>` component (framework specific) or `loadImageBySize`.
*   **Write**: use the `createImage` function from `jazz-tools/media`

```tsx
<Image imageId={productImage.$jazz.id} width={300} />
```

## Troubleshooting

### "Data not appearing" or "Value is undefined"
1. **Check `$isLoaded`**: Ensure you are checking `if (coValue.$isLoaded)` before rendering.
2. **Deep Loading**: If nested data is missing, use `resolve` in your hook (e.g. `useCoState(..., { resolve: { items: true } })`).
3. **Provider Check**: Ensure the component is wrapped in the appropriate `<JazzProvider />`.

### "Infinite re-render loops"
1. **Selector Stability**: Ensure your `select` function in `useCoState` is stable or returns stable references.
2. **Expensive Selectors**: Move computations to `useMemo` instead of doing them inside the `select` function.
3. **Equality Function**: Use `equalityFn` to prevent unnecessary updates if the selected data hasn't changed semantically.

### "Reactivity not triggering"
1. **Direct Mutations**: Ensure you are using `.$jazz.set()` or `.$jazz.push()` instead of direct property assignment (e.g. `obj.key = val` won't sync).
2. **Deep Updates**: Remember that updating a nested scalar doesn't trigger a change in the parent CoValue unless that parent property is replaced.