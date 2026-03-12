---
marp: true
title: How Chat uses Jazz
theme: jazz
paginate: true
html: true
---

<!-- _class: hero -->

# How Chat uses Jazz

A walkthrough of a real-time, permission-aware chat app — built with Jazz and React.

Public rooms, private chats, invite links, emoji reactions, file uploads, collaborative canvases.

![bg contain right:45%](screenshots/01-chat-view.png)

---

## What is Jazz?

Jazz is a **local-first** sync framework. Every client runs a full database in a WASM worker, persisted to disk via OPFS. Changes sync to an edge server and fan out to all connected clients in real time.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 560 212" width="520" height="196" style="display:block;margin:0.5rem auto">
  <defs>
    <marker id="arr" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#6b7280"/></marker>
    <marker id="arrs" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto-start-reverse"><polygon points="0 0, 8 3, 0 6" fill="#6b7280"/></marker>
  </defs>
  <rect x="180" y="10" width="200" height="58" rx="8" fill="#dcfce7" stroke="#16a34a" stroke-width="1.5"/>
  <text x="280" y="34" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="13" font-weight="700" fill="#166534">Jazz sync server</text>
  <text x="280" y="54" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="11" fill="#166534">sync + fan-out + policy enforcement</text>
  <rect x="8" y="130" width="170" height="74" rx="8" fill="#dbeafe" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="93" y="154" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="13" font-weight="700" fill="#1e40af">Browser A</text>
  <text x="93" y="174" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">WASM worker</text>
  <text x="93" y="192" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">OPFS (local DB)</text>
  <rect x="382" y="130" width="170" height="74" rx="8" fill="#dbeafe" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="467" y="154" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="13" font-weight="700" fill="#1e40af">Browser B</text>
  <text x="467" y="174" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">WASM worker</text>
  <text x="467" y="192" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">OPFS (local DB)</text>
  <line x1="215" y1="68" x2="93" y2="128" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3" marker-start="url(#arrs)" marker-end="url(#arr)"/>
  <line x1="345" y1="68" x2="467" y2="128" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3" marker-start="url(#arrs)" marker-end="url(#arr)"/>
</svg>

Writes are **instant locally** — sync happens in the background. Row-level **policies** are enforced on the server; only authorised data reaches the local store.

---

<!-- _style: "pre { font-size: 0.65rem; line-height: 1.45; margin: 0; } h2 { margin-bottom: 0.5em; }" -->

## The schema

**[`schema/current.ts`](../schema/current.ts)** — source of truth. `jazz-tools build` produces a SQL migration and typed query-builder interfaces.

<div style="display:grid;grid-template-columns:1fr 1fr;gap:0.8rem;margin-top:0.4rem">

```typescript
table("profiles", {
  userId: col.string(),
  name: col.string(),
  avatar: col.string().optional(),
});
table("chats", {
  isPublic: col.boolean(),
  createdBy: col.string(),
  joinCode: col.string().optional(),
});
table("chatMembers", {
  chat: col.ref("chats"),
  userId: col.string(),
  joinCode: col.string().optional(),
});
table("messages", {
  chat: col.ref("chats"),
  text: col.string(),
  sender: col.ref("profiles"),
  senderId: col.string(),
  createdAt: col.timestamp(),
});
```

```typescript
table("reactions", {
  message: col.ref("messages"),
  userId: col.string(),
  emoji: col.string(),
});
table("canvases", {
  chat: col.ref("chats"),
  createdAt: col.timestamp(),
});
table("strokes", {
  canvas: col.ref("canvases"),
  ownerId: col.string(),
  color: col.string(),
  width: col.int(),
  pointsJson: col.string(),
  createdAt: col.timestamp(),
});
table("attachments", {
  message: col.ref("messages"),
  type: col.string(),
  name: col.string(),
  data: col.string(),
  mimeType: col.string(),
  size: col.int(),
});
```

</div>

---

## Client setup

One call to `createJazzClient` initialises the WASM worker, opens the OPFS database, and begins syncing. `JazzProvider` makes the `db` handle available to every component.

**[`src/App.tsx`](../src/App.tsx)**

```typescript
import { createJazzClient, JazzProvider } from "jazz-tools/react";

const client = createJazzClient({
  appId:     import.meta.env.VITE_JAZZ_APP_ID ?? "chat-react-example",
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL,
});

export function App() {
  return (
    <JazzProvider client={client}>
      <AppContent />
    </JazzProvider>
  );
}
```

That's the entire setup. Jazz handles the WebSocket, the local store, and the sync layer.

---

## Accessing the db anywhere

`useDb()` and `useSession()` are available to any component inside `JazzProvider`.

```typescript
import { useDb, useSession, useAll } from "jazz-tools/react";

const db = useDb(); // full query + write API
const session = useSession(); // { user_id, ... } | null
const userId = session?.user_id;
```

`ChatList`, `ChatView`, `MessageComposer`, `ActionMenu`, `ChatReactions` — each calls `useDb()` directly, no wiring needed.

---

## Live queries with `useAll`

![bg contain right:38%](screenshots/03-message-sent.png)

`useAll` subscribes to a live query against the local database and re-renders the component on every change, from any user, anywhere.

**[`src/components/chat-view/ChatView.tsx`](../src/components/chat-view/ChatView.tsx)**

```typescript
const messages =
  useAll(app.messages.where({ chat: chatId }).orderBy("createdAt", "desc").limit(20)) ?? [];
```

When a remote user sends a message, it appears instantly. Jazz pushes the change to every subscriber automatically.

---

## Synchronous writes

![bg contain right:38%](screenshots/02-composing.png)

Writes to the local database return immediately. Sync to the edge server happens in the background.

**[`src/components/composer/MessageComposer.tsx`](../src/components/composer/MessageComposer.tsx)**

```typescript
const handleSend = (html: string) => {
  db.insert(app.messages, {
    chat: chatId,
    text: html.trim(),
    sender: myProfile.id,
    senderId: userId,
    createdAt: new Date(),
  });
};
```

The local write is the source of truth. The server catches up silently.

---

## Permissions — the policy DSL

Policies live in **[`schema/permissions.ts`](../schema/permissions.ts)**, written in a typed DSL. They compile to a policy AST embedded in the schema and are enforced server-side on every sync request.

```typescript
// Messages: only chat members can read or send
policy.messages.allowRead.where((msg) =>
  anyOf([
    allowedTo.read("chat"), // inherits from parent: public chats are readable
    policy.chatMembers.exists.where({ chat: msg.chat, userId: session.user_id }),
  ]),
);
policy.messages.allowInsert.where((msg) =>
  policy.chatMembers.exists.where({ chat: msg.chat, userId: session.user_id }),
);
policy.messages.allowDelete.where({ senderId: session.user_id });
```

Row-level security is a schema concern. Components contain no auth logic.

---

## Public and private chats

![bg contain right:38%](screenshots/08-chat-list.png)

The `chats` policy layers three access conditions without any backend logic:

```typescript
policy.chats.allowRead.where((chat) =>
  anyOf([
    { isPublic: true }, // public rooms
    policy.chatMembers.exists.where({
      // accepted members
      chat: chat.id,
      userId: session.user_id,
    }),
    { joinCode: session["claims.join_code"] }, // invite link bearer
  ]),
);
```

The `claims.join_code` condition is what makes the invite flow work — a client can present a join code as an ephemeral session claim without being a member yet.

---

## The invite flow

Private chats carry a `joinCode`. Sharing `/#/invite/:chatId/:code` lets anyone join in two steps.

**[`src/components/InviteHandler.tsx`](../src/components/InviteHandler.tsx)**

```typescript
// Step 1: subscribe with the join code as an ephemeral session claim.
// The server's chats policy matches { joinCode: session["claims.join_code"] }
// and syncs the chat row locally — satisfying the FK constraint before INSERT.
db.subscribeAll(
  app.chats.where({ id: chatId }),
  (delta) => {
    if (delta.all.length > 0) setChatReady(true);
  },
  undefined,
  { user_id: userId, claims: { join_code: code } },
);

// Step 2: once the chat row is local, insert the membership and navigate.
db.insert(app.chatMembers, { chat: chatId, userId, joinCode: code });
navigate(`/#/chat/${chatId}`);
```

The claim is never stored — it exists only for this subscription's lifetime.

---

## Reactions — live, policy-scoped

![bg contain right:38%](screenshots/04-reaction-picker.png)

Each reaction is a row. The query is live; toggling one is a synchronous insert or delete.

**[`src/components/chat/ChatReactions.tsx`](../src/components/chat/ChatReactions.tsx)**

```typescript
const reactions = useAll(app.reactions.where({ message: messageId })) ?? [];

const handleToggle = (emoji: string) => {
  const mine = reactions.find((r) => r.emoji === emoji && r.userId === userId);
  if (mine) {
    db.delete(app.reactions, mine.id);
  } else {
    db.insert(app.reactions, { message: messageId, userId, emoji });
  }
};
```

`allowDelete` is scoped to `{ userId: session.user_id }` — ownership is enforced by the policy itself, with no access logic needed in the component.

---

## Attachments — files as rows

File uploads are stored as base64 strings directly in the `attachments` table.

```typescript
const message = db.insert(app.messages, {
  chat: chatId,
  text: "",
  sender: myProfile.id,
  senderId: userId,
  createdAt: new Date(),
});

db.insert(app.attachments, {
  message: message.id,
  type: attachment.type,
  name: attachment.name,
  data: attachment.data, // base64
  mimeType: attachment.mimeType,
  size: attachment.size,
});
```

Attachments inherit their read policy from the parent message via `allowedTo.read("message")`.

---

## Collaborative canvas

![bg contain right:38%](screenshots/06-canvas.png)

Each chat can host shared drawing canvases. Strokes are rows synced in real time.

```typescript
// Live — re-renders whenever any stroke is added or removed
const allStrokes = useAll(app.strokes.where({ canvas: canvasId })) ?? [];

// Clear your own strokes — policy enforces ownerId check server-side
for (const s of allStrokes.filter((s) => s.ownerId === userId)) {
  db.delete(app.strokes, s.id);
}
```

Strokes inherit read access from their canvas, which inherits from the chat. `allowDelete` is scoped to `{ ownerId: session.user_id }`. The canvas component has no explicit access checks.

---

## Fire-and-forget writes

All writes in this app use the synchronous path: `db.insert` / `db.delete` return immediately. Sync to the edge server happens in the background.

```typescript
db.insert(app.chatMembers, { chat: chatId, userId, joinCode: code });
navigate(`/#/chat/${chatId}`);
```

The membership row is written locally first. The local database satisfies queries instantly while the server catches up.

---

## Jazz API surface — used in Chat

| API                                         | Notes                                                     |
| ------------------------------------------- | --------------------------------------------------------- |
| `createJazzClient`                          | Initialises WASM worker + OPFS database, begins syncing   |
| `JazzProvider`                              | Provides `db` to every component                          |
| `useDb()`                                   | Db handle — callable from any component in the tree       |
| `useSession()`                              | Current user identity (`user_id`)                         |
| `useAll(query)`                             | React hook — live query, re-renders on every change       |
| `db.insert` / `db.delete`                   | Synchronous local writes — sync to edge in the background |
| `db.subscribeAll(query, cb, opts, session)` | Manual subscription with optional session-claim override  |
| `definePermissions`                         | Policy DSL — row-level security compiled into the schema  |
