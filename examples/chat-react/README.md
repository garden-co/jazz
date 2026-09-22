# Chat

Real-time, permission-aware chat app. Public rooms, private chats with invite links, emoji reactions, and collaborative drawing canvases. Jazz handles sync and row-level security; React renders the UI.

## Getting started

```bash
pnpm install
pnpm dev        # starts the Jazz server, pushes the schema, and opens Vite
```

## Commands

```bash
pnpm test               # Vitest browser tests
pnpm build              # Optional schema validation + production build
```

## How it works

**State sync** is entirely handled by Jazz. Every message, reaction, stroke, and membership change is a synchronous local write (`db.insert`, `db.delete`). Jazz replicates the change to all connected peers in the background. The UI is driven by `useAll` reactive queries — no polling, no manual state management.

**Row-level security** is a schema concern, not an application concern. Policies live in `permissions.ts` in a typed DSL. They compile into a policy AST enforced server-side on every sync request. Components contain no auth logic.

**Public chats** are visible to all connected clients. **Private chats** are restricted to members. A trusted provider's `join_code` claim may grant an initial read, but the local-first invite flow does not mint or override provider claims.

**The invite flow** submits a membership row containing the current account ID and the supplied code. The server requires that account to match the author and checks that the chat is public, belongs to the account, or has the matching invite code. Only after the membership write settles does the handler navigate to the chat. Knowing a private chat's ID alone is insufficient to join.

**Collaborative canvases** attach to a chat. Strokes are rows, synced in real time. Delete access compares `$createdBy.account` with `session.user.account`, so another linked identity on the same account retains access; the canvas component has no explicit access checks.

## Schema

Defined in `schema.ts` using the Jazz typed schema DSL. Running `pnpm build` validates `schema.ts` before the production build; the app imports the typed `app` export directly from that file.

- **profiles** — userId, name, avatar
- **chats** — isPublic, joinCode (nullable — set for private chats); authorship comes from `$createdBy`
- **chatMembers** — chat (ref), userId, joinCode
- **messages** — chat (ref), text, sender (ref), senderId; display order/time comes from `$createdAt`
- **reactions** — message (ref), userId, emoji
- **canvases** — chat (ref)
- **strokes** — canvas (ref), color, width, pointsJson; authorship/time comes from `$createdBy`/`$createdAt`
