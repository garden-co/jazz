# Chat

Real-time, permission-aware chat app. Public rooms, private chats with invite links, emoji reactions, file attachments, and collaborative drawing canvases. Jazz handles sync and row-level security; React renders the UI.

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
pnpm build              # Schema codegen + production build
```

## How it works

**State sync** is entirely handled by Jazz. Every message, reaction, stroke, and membership change is a synchronous local write (`db.insert`, `db.delete`). Jazz replicates the change to all connected peers in the background. The UI is driven by `useAll` reactive queries — no polling, no manual state management.

**Row-level security** is a schema concern, not an application concern. Policies live in `schema/permissions.ts` in a typed DSL. They compile into a policy AST enforced server-side on every sync request. Components contain no auth logic.

**Public chats** are visible to all connected clients. **Private chats** are restricted to members. A chat carries a `joinCode` column; presenting the code as an ephemeral session claim grants read access before membership is confirmed, which is how invite links work without a round-trip to a backend.

**The invite flow** works in two steps: `InviteHandler` subscribes to the chat with `{ claims: { join_code: code } }` as a session override. The server matches `chat.joinCode = @session.claims.join_code` and syncs the chat row locally. Once the row is present (FK constraint satisfied), the handler inserts the `chatMembers` row and navigates to the chat.

**Attachments** are stored as base64 strings in the `attachments` table, linked to their parent message. They inherit their read policy from the message via `allowedTo.read("message")` — no separate asset server required.

**Collaborative canvases** attach to a chat. Strokes are rows, synced in real time. Delete access is scoped to `{ ownerId: session.user_id }`; the canvas component has no explicit access checks.

## Schema

Defined in `schema/current.ts` using the Jazz `table()` / `col.*` DSL. Running `pnpm build` generates the typed client (`schema/app.ts`) and SQL files.

- **profiles** — userId, name, avatar
- **chats** — isPublic, createdBy, joinCode (nullable — set for private chats)
- **chatMembers** — chat (ref), userId, joinCode
- **messages** — chat (ref), text, sender (ref), senderId, createdAt
- **reactions** — message (ref), userId, emoji
- **canvases** — chat (ref), createdAt
- **strokes** — canvas (ref), ownerId, color, width, pointsJson, createdAt
- **attachments** — message (ref), type, name, data (base64), mimeType, size
