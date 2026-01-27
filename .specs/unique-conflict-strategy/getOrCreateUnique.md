# getOrCreateUnique - Design Document

## Overview

`getOrCreateUnique` is a method available on `CoMap`, `CoList`, and `CoFeed` that provides deterministic, conflict-free creation of unique CoValues. When multiple users create a CoValue with the same uniqueness key (even offline), the system guarantees that all nodes converge to the same state.

**Key guarantees:**
- Deterministic ID generation from uniqueness key
- First-init-wins conflict resolution for concurrent creation
- Automatic merging of subsequent updates
- Nested CoValues inherit derived uniqueness

## Conflict Resolution

### The Problem: Concurrent Creation

When two users create the same unique CoValue simultaneously (or while offline), both create valid CoValues with the same ID. Without conflict resolution, this would lead to divergent states.

```
Timeline:
─────────────────────────────────────────────────────────────────

Alice (offline)                    Bob (offline)
     │                                  │
     │  Creates UserSettings            │  Creates UserSettings
     │  unique: "settings-123"          │  unique: "settings-123"
     │  theme: "dark"                   │  theme: "light"
     │  madeAt: 1706000100              │  madeAt: 1706000200
     │                                  │
     └──────────────┬───────────────────┘
                    │
              Both go online
                    │
                    ▼
         Transactions are synced
                    │
                    ▼
    ┌───────────────────────────────────┐
    │   Conflict Resolution:            │
    │   Alice's init wins (earlier)     │
    │   theme: "dark"                   │
    └───────────────────────────────────┘
```

### First-Init-Wins Strategy

The system uses a **first-init-wins** strategy for creation conflicts:

1. **Init transactions** are marked with `meta: { init: true }`
2. When multiple init transactions exist, the one with the earliest `madeAt` timestamp wins
3. If timestamps are equal, the `sessionID` is used as a deterministic tiebreaker
4. Losing init transactions are marked as invalid and excluded from content computation

```typescript
// Internal: How init transactions are compared
function compareInitTransactions(a, b) {
  // 1. Earlier timestamp wins
  if (a.madeAt !== b.madeAt) {
    return a.madeAt - b.madeAt;
  }

  // 2. Same timestamp: use sessionID for determinism
  return a.txID.sessionID.localeCompare(b.txID.sessionID);
}
```

### What Gets Resolved

| Scenario | Resolution |
|----------|------------|
| Different `madeAt` timestamps | Earlier timestamp wins |
| Same `madeAt`, different sessions | Lexicographically smaller sessionID wins |
| Same session, same timestamp | Earlier txIndex wins |

## Conflict Resolution by Value Type

### Primitive Fields (strings, numbers, booleans)

Primitive fields use **last-write-wins (LWW)** semantics after the init conflict is resolved.

```typescript
const Counter = co.map({
  value: z.number(),
  lastUpdatedBy: z.string(),
});

// Alice creates (wins init conflict - earlier timestamp)
const aliceCounter = await Counter.getOrCreateUnique({
  value: { value: 0, lastUpdatedBy: "alice" },
  unique: "shared-counter",
  owner: group,
});

// Bob creates (loses init conflict - later timestamp)
// His init values are discarded
const bobCounter = await Counter.getOrCreateUnique({
  value: { value: 100, lastUpdatedBy: "bob" },  // These values are ignored
  unique: "shared-counter",
  owner: group,
});

// After sync, both see Alice's initial values:
// { value: 0, lastUpdatedBy: "alice" }

// Subsequent updates use LWW:
aliceCounter.$jazz.set("value", 5);   // madeAt: 1000
bobCounter.$jazz.set("value", 10);    // madeAt: 1001

// After sync, both see: { value: 10 } (Bob's update was later)
```

### Non-Conflicting Field Updates

When users update different fields, all updates are preserved:

```typescript
const Profile = co.map({
  name: z.string(),
  bio: z.string().optional(),
  avatar: z.string().optional(),
});

// Both create the same profile (Alice wins init)
const aliceProfile = await Profile.getOrCreateUnique({
  value: { name: "Shared Profile" },
  unique: "profile-123",
  owner: group,
});

const bobProfile = await Profile.getOrCreateUnique({
  value: { name: "Shared Profile" },
  unique: "profile-123",
  owner: group,
});

// Alice updates bio
aliceProfile.$jazz.set("bio", "Hello world");

// Bob updates avatar
bobProfile.$jazz.set("avatar", "avatar.png");

// After sync, both see ALL updates merged:
// { name: "Shared Profile", bio: "Hello world", avatar: "avatar.png" }
```

### Nested CoValues (References)

Nested CoValues with `sameAsContainer` permission inherit derived uniqueness:

```typescript
const NotificationSettings = co.map({
  email: z.boolean(),
  push: z.boolean(),
  frequency: z.enum(["daily", "weekly", "instant"]),
});

const UserSettings = co.map({
  theme: z.enum(["light", "dark"]),
  notifications: NotificationSettings.withPermissions({
    onInlineCreate: "sameAsContainer",  // Required for derived uniqueness
  }),
});

// Alice creates settings with nested notifications
const aliceSettings = await UserSettings.getOrCreateUnique({
  value: {
    theme: "dark",
    notifications: { email: true, push: false, frequency: "daily" },
  },
  unique: "user-settings-123",
  owner: group,
  resolve: { notifications: true },
});

// Bob creates the same settings (offline)
const bobSettings = await UserSettings.getOrCreateUnique({
  value: {
    theme: "light",
    notifications: { email: false, push: true, frequency: "weekly" },
  },
  unique: "user-settings-123",
  owner: group,
  resolve: { notifications: true },
});

// Both parent AND nested CoValues have the same IDs!
console.log(aliceSettings.$jazz.id === bobSettings.$jazz.id);  // true
console.log(aliceSettings.notifications.$jazz.id === bobSettings.notifications.$jazz.id);  // true

// After sync:
// - Parent uses Alice's init values (theme: "dark")
// - Nested also uses Alice's init values (email: true, push: false, frequency: "daily")

// Updates to nested values also merge:
aliceSettings.notifications.$jazz.set("frequency", "instant");
bobSettings.notifications.$jazz.set("push", true);

// After sync, notifications = { email: true, push: true, frequency: "instant" }
```

### Derived Uniqueness for Nested CoValues

The uniqueness key is derived by appending the field name:

```typescript
// Parent uniqueness (string):
unique: "user-settings-123"

// Child uniqueness:
unique: "user-settings-123/notifications"

// Grandchild uniqueness:
unique: "user-settings-123/notifications/advanced"
```

For object-based uniqueness:

```typescript
// Parent uniqueness (object):
unique: { userId: "abc", tenantId: "xyz" }

// Child uniqueness:
unique: { userId: "abc", tenantId: "xyz", _field: "notifications" }

// Grandchild uniqueness:
unique: { userId: "abc", tenantId: "xyz", _field: "notifications/advanced" }
```

### CoList Items

CoList items follow the same rules - the list structure itself has init conflict resolution, and subsequent item additions/removals use CRDT semantics:

```typescript
const Task = co.map({
  title: z.string(),
  done: z.boolean(),
});

const TaskList = co.list(co.ref(Task));

// Alice creates the list (wins init)
const aliceList = await TaskList.getOrCreateUnique({
  value: [Task.create({ title: "Task A", done: false }, { owner: group })],
  unique: "tasks-123",
  owner: group,
});

// Bob creates the same list (loses init - his initial items are discarded)
const bobList = await TaskList.getOrCreateUnique({
  value: [Task.create({ title: "Task B", done: false }, { owner: group })],
  unique: "tasks-123",
  owner: group,
});

// After sync, list only contains "Task A" (from Alice's winning init)

// But subsequent additions from both are preserved:
aliceList.push(Task.create({ title: "Task C", done: false }, { owner: group }));
bobList.push(Task.create({ title: "Task D", done: false }, { owner: group }));

// After sync, list contains: ["Task A", "Task C", "Task D"]
// (Order depends on CRDT semantics)
```

### CoFeed Entries

CoFeed is append-only, so all entries from all users are preserved:

```typescript
const Message = co.map({
  text: z.string(),
  timestamp: z.number(),
});

const ChatFeed = co.feed(co.ref(Message));

// Both create the same feed
const aliceFeed = await ChatFeed.getOrCreateUnique({
  value: [],
  unique: "chat-room-123",
  owner: group,
});

const bobFeed = await ChatFeed.getOrCreateUnique({
  value: [],
  unique: "chat-room-123",
  owner: group,
});

// Both add messages
aliceFeed.push(Message.create({ text: "Hello", timestamp: Date.now() }, { owner: group }));
bobFeed.push(Message.create({ text: "Hi there", timestamp: Date.now() }, { owner: group }));

// After sync, feed contains BOTH messages
// Each user's entries are in their own "per-account" stream
```
