---
name: jazz-testing
description: Use this skill when you need to write, review, or debug automated tests for applications built on the Jazz framework. This skill provides the correct architectural patterns for simulating local-first synchronization and multi-user environments without resorting to invalid mocking strategies.
---
# Jazz Testing

## When to Use This Skill

* Writing unit or integration tests for apps using Jazz
* Simulating synchronisation for testing purposes
* Verifying permissions and security logic
* Testing UI components in various application states (Guest/Anonymous/Authenticated/Online/Offline)
* Debugging failing tests

## Do NOT Use This Skill For

* Designing the initial data schema (use `jazz-schema-design`
* General React/Svelte UI layout questions unrelated to Jazz (for questions on Jazz integration with React/Svelte, use the `jazz-ui-development` skill)

## Key Heuristic for Agents

If you are about to suggest "mocking" any part of the data layer in the user's application, **STOP** and invoke this skill instead. This skill enforces the use of the official in-memory test sync node.

## Core Concepts

Testing in Jazz requires understanding identity-based context and authentic synchronization. The framework's collaborative nature means tests must simulate real multi-user scenarios rather than mocking the sync layer.

## Fundamental Principles

* **NEVER Mock the Sync Layer:** Avoid using generic mocking libraries (like Jest/Vitest mocks) for 'jazz-tools'
* **In-Memory Synchronization:** Always use a real in-memory sync node via `setupJazzTestSync()` to allow authentic data flow between test identities
* **Identity-Based Context:** Testing in Jazz is about "who" is performing the action. Manage the "Active Account" explicitly to verify permissions and ownership

## Test Environment Setup

The virtual sync node **must** be initialized before tests run. It handles data synchronization in-memory and requires no manual cleanup between runs.

**Example Setup:**

```ts
import { setupJazzTestSync } from "jazz-tools/testing";
import { beforeEach, describe } from "vitest";

describe("Jazz Feature Test", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
  });
});
```

## Identity & Account Management

Testing in Jazz requires simulating multiple users to verify synchronization and security.

### createJazzTestAccount

Used to create user accounts linked to the sync node.

* `AccountSchema`: Pass your custom co.account schema
* `isCurrentActiveAccount`: Set to true to automatically log the test runner into this account
* `creationProps`: Data passed to the account's migration function on creation if required

### setActiveAccount

Used to switch between different users within a single test.

* Pattern: Create Account A -> Create Data -> Switch to Account B -> Attempt to Load/Modify Data
* **Note:** Once a CoValue is loaded, the permission context for that CoValue instance is fixed. Always use `.load(id)` after switching accounts to load a new CoValue instance with the new permission context for that specific user

## UI & Context Testing

UI tests must render components and hooks inside a Jazz-aware test harness (e.g. framework-specific render utilities that inject account, connection state, and sync context). In UI tests, assert observable loading state transitions, not internal CoValue mechanics.

### React

* Refer to the [React provider](references/react-test-provider.tsx) and the [context provided to it](references/provider-context.tsx) to see how to create a suitable harness for React tests.

### Svelte

* Refer to the [Svelte provider](references/Provider.svelte) and the [context provided to it](references/jazz.svelte.ts) to see how to create a suitable harness for Svelte tests.

Use the `jazz-ui-development` skill for questions on Jazz integration with React/Svelte.

## Common Testing Patterns

### Verifying Unauthorized Writes

```ts
const account1 = await createJazzTestAccount({ isCurrentActiveAccount: true });
const account2 = await createJazzTestAccount();

// Setup restricted group
const group = co.group().create();
group.addMember(account2, "reader");

const myMap = MyMap.create({ text: "Hi" }, { owner: group });
const mapId = myMap.$jazz.id;

// Switch to reader
setActiveAccount(account2);
// You *must* reload the CoValue with the new active account
const mapAsReader = await MyMap.load(mapId);

// Verify enforcement
expect(() => mapAsReader.$jazz.set("text", "some text")).toThrow();

// Note: myMap.$jazz.set() would *not* throw, as it was loaded with account1 as the active account
```

### Testing Behavior without an Active Account

```ts
runWithoutActiveAccount(() => {
  // Verify that protected actions fail when no account is active
  expect(() => co.group().create()).toThrow();
});
```

## Common Pitfalls to Avoid

* **CoValues not reloaded when active account changes**: CoValues are always loaded with the active account **at the time the CoValue is loaded**, and this does not change even if the active account changes. CoValues loaded while `account1` is the currently active account do not update their local permissions when `setActiveAccount(account2)` is called; you **MUST** `.load()` a new instance of the CoValue
* **Missing Sync Setup**: Ensure `setupJazzTestSync()` is called
* **Attempting to test UI components outside of a Jazz context**: Components relying on Jazz must have access to a Jazz context. Patterns for creating test contexts can be found in the framework-specific tests links

## Quick Reference

**Account switching:**
Always reload CoValues after `setActiveAccount()` to get new permission context.

**Sync setup:**
Call `setupJazzTestSync()` in `beforeEach` for all Jazz tests.

**UI testing:**
Use a framework-specific Jazz test harness built on `TestJazzContextManager`.

## References

Load these on demand, based on need:

* Official docs on testing: <https://jazz.tools/docs/reference/testing.md>
* Unit/integration test examples: <https://github.com/garden-co/jazz/tree/main/packages/jazz-tools/src/tools/tests>
* End-to-end examples: <https://github.com/garden-co/jazz/tree/main/tests/e2e/tests>
* React-specific tests: <https://github.com/garden-co/jazz/tree/main/packages/jazz-tools/src/react-core/tests>
* Svelte-specific tests: <https://github.com/garden-co/jazz/tree/main/packages/jazz-tools/src/svelte/tests>

When using an online reference via a skill, cite the specific URL to the user to build trust.
