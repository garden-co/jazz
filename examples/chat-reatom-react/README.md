# Chat (Reatom + React)

Real-time, permission-aware chat app. Public rooms, private chats with invite links, emoji reactions, file attachments, and collaborative drawing canvases. Jazz handles sync and row-level security; **Reatom** owns all reactive state; React renders the UI.

This is a port of the `chat-react` example. Every React hook (`useAll`, `useDb`, `useSession`) is replaced with reatom atoms and actions. The binding lives in `src/lib/reatom-jazz/`.

## Getting started

```bash
pnpm install
pnpm dev        # starts the Jazz server, pushes the schema, and opens Vite
```

To understand how the app uses Jazz, run the walkthrough:

```bash
pnpm walkthrough
```

## Commands

```bash
pnpm walkthrough        # Marp slideshow — Jazz patterns used in this app
pnpm walkthrough:shots  # Re-capture screenshots for the slideshow
pnpm test               # Vitest browser tests
pnpm build              # Optional schema validation + production build
```

## How it works

### Jazz ↔ Reatom binding

The binding (`src/lib/reatom-jazz/index.ts`) exposes two primitives on top of `createJazzClient`:

- **`reatomQueryAll(builder, name)`** — wraps a Jazz query into a suspensible atom. The atom resolves via `withSuspenseInit` when the underlying `CacheEntry` promise settles, then stays in sync through `withConnectHook` + `entry.subscribe`. Equivalent of `useAll` / `useAllSuspense`, but the subscription lifetime is tied to atom consumers, not component mounts.

- **`reatomCachedQuery(builder, nameOrOptions)`** — factory for per-argument query atoms. Returns a function `(...args) => Atom<T[]>` that creates and caches one `reatomQueryAll` atom per unique argument set (compared with `isShallowEqual` by default). This replaces patterns like `useAll(app.messages.where({ chatId }))` inside components — instead you call `getChatMessagesQuery(chatId)()` and the atom is reused across renders and components.

The Jazz client itself is a suspensible computed atom created by `createJazz(config)`. Components access it as `jazz()` — returns `{ db, session, manager }`.

### State and queries

All queries live in `src/model/queries.ts` as `reatomCachedQuery` factories:

```ts
const getChatRowsQuery = jazz.reatomCachedQuery(
  (chatId: string) => app.chats.where({ id: chatId }),
  "chatRows",
);

// in a component:
const rows = getChatRowsQuery(chatId)();
```

Module-scope queries (no parameters) use `reatomQueryAll` directly — e.g. `allProfilesQuery`, `myMembershipsQuery`.

Derived state like `myProfile` (`src/model/my-profile.ts`) is a `computed` atom that reads query atoms and auto-creates the profile row via `withConnectHook` + `effect` on first access.

### Components

Every component is wrapped in `reatomComponent` from `@reatom/react` — this tracks atom reads and re-renders only when subscribed atoms change. Mutations go through `action` or `wrap`:

```ts
const handleDelete = wrap((messageId: string) => {
  db.delete(app.messages, messageId);
});
```

### Routing

Hash-based routing via `reatomRoute` + `setupHashUrl()`. The route tree lives in `src/routes.tsx`. Navigation: `chatRoute.go({ chatId })` or `<a href={chatRoute.path({ chatId })}>`.

### State sync

Entirely handled by Jazz. Every message, reaction, stroke, and membership change is a synchronous local write (`db.insert`, `db.delete`). Jazz replicates the change to all connected peers in the background. The UI is driven by `reatomQueryAll` / `reatomCachedQuery` reactive atoms — no polling, no manual state management.

**Row-level security** is a schema concern, not an application concern. Policies live in `permissions.ts` in a typed DSL. They compile into a policy AST enforced server-side on every sync request. Components contain no auth logic.

**Public chats** are visible to all connected clients. **Private chats** are restricted to members. A chat carries a `joinCode` column; presenting the code as an ephemeral session claim grants read access before membership is confirmed, which is how invite links work without a round-trip to a backend.

**The invite flow** works in two steps: `InviteHandler` subscribes to the chat with `{ claims: { join_code: code } }` as a session override. The server matches `chat.joinCode = @session.claims.join_code` and syncs the chat row locally. Once the row is present (FK constraint satisfied), the handler inserts the `chatMembers` row and navigates to the chat.

**Attachments** are stored as base64 strings in the `attachments` table, linked to their parent message. They inherit their read policy from the message via `allowedTo.read("message")` — no separate asset server required.

**Collaborative canvases** attach to a chat. Strokes are rows, synced in real time. Delete access is scoped to `{ ownerId: session.user_id }`; the canvas component has no explicit access checks.

## Schema

Defined in `schema.ts` using the Jazz typed schema DSL. Running `pnpm build` validates `schema.ts` before the production build; the app imports the typed `app` export directly from that file.

- **profiles** — userId, name, avatar
- **chats** — isPublic, createdBy, joinCode (nullable — set for private chats)
- **chatMembers** — chat (ref), userId, joinCode
- **messages** — chat (ref), text, sender (ref), senderId, createdAt
- **reactions** — message (ref), userId, emoji
- **canvases** — chat (ref), createdAt
- **strokes** — canvas (ref), ownerId, color, width, pointsJson, createdAt
- **attachments** — message (ref), type, name, data (base64), mimeType, size
