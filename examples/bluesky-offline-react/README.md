# Jazz ❤️ Bluesky — Local-first ATProto

A proof of concept showing how Jazz can be layered over ATProto. This Jazz-backed app implements a subset of the features which exist on a normal Bluesky app view, showing how you can add Jazz to an existing application or stack to give local-first, offline-capable reactive views into data which does not natively support it.

- ATProto remains the source of truth for posts, likes, and reposts.
- A slim Hono server performs authenticated PDS writes.
- It exposes an `/api/timeline` endpoint to fetch AppView feed events and write sync them into Jazz.
- The data itself is fetched through Jazz directly: React uses one reactive query subscription for timeline events, posts, profiles, images, threads, likes, and reposts.
- Posts and reaction intentions are written to Jazz first. If the online PDS write fails, they remain queued and retry after connectivity returns

## Data flow

```text
READ
Bluesky AppView ──20 events──> Bluesky adapter ──> Jazz bridge ──rows──> Jazz ──useAll──> React

WRITE
Bluesky PDS <──── Bluesky adapter <──── Jazz bridge <──── pending Jazz intentions <──── React
```

`/api/timeline` is only a trigger. It asks Bluesky for one bounded page and returns cursor metadata immediately, rather than returning posts to React directly. The bridge projects rows into Jazz incrementally as they arrive, and they sync to the client over the normal path.

> [!NOTE]
The 'poll' from the client could easily be replaced with a 'push' mechanism. For example, a firehose consumer could project rows into Jazz, and these would sync through the reactive subscription to Jazz directly, without any separate messaging channel with the client.

## Architecture

The example keeps the boundary between the authoritative system and Jazz deliberately narrow:

| Component | Responsibility | Knows about Jazz? | Knows about Bluesky? |
| --- | --- | --- | --- |
| `server/app.ts` | HTTP routes and request validation | No | Only session-shaped route inputs |
| `server/auth.ts` | OAuth sessions and Jazz JWTs | Only JWT issuance | Yes |
| `server/bluesky.ts` | Read from AppView; write to the PDS | No | Yes |
| `server/timeline.ts` | Pure Bluesky-to-projection normalization | Only the projection shape | Yes |
| `server/bridge.ts` | Project reads into Jazz and reconcile queued intentions | Yes | Calls the Bluesky adapter |
| `schema.ts` | Local relational projection and pending intentions | Yes | No protocol calls |
| `src/Timeline.tsx` | One reactive Jazz query plus local-first commands | Yes | Only calls trigger/reconcile routes |
| `src/timeline-model.ts` | Pure rows-to-thread view model | No | No |
| `src/TimelineView.tsx` | Presentational React components | No | No |

The bridge exposes three application-level operations:

- `projectTimelinePage`: read a bounded authoritative page and progressively project it into Jazz.
- `projectThread`: lazily read one thread and add it to the same projection.
- `reconcileOperations`: apply queued local intentions to the authoritative system, then update Jazz with the result.

For illustration purposes, Bluesky-specific OAuth, XRPC endpoints, AT URIs, records, and TIDs stay on one side. Jazz tables, permissions, reactive transport, and pending-operation rows stay on the other.

## Layering Jazz over another database

For a conventional SQL-backed application, keep the Jazz-facing projection and client pattern. Replace `server/bluesky.ts` with an adapter around the existing service or database, and replace the small source-specific normalisation/reconciliation mapping in `server/timeline.ts` and `server/bridge.ts`:

1. Read a stable page from the existing database using its normal cursor or primary key.
2. Normalise those records into the projection rows expected by the bridge.
3. Upsert them into Jazz with deterministic IDs so repeated reads and multiple users deduplicate.
4. Let clients render only the reactive Jazz query; do not merge an HTTP response into separate client state.
5. Represent offline writes as intention rows in Jazz.
6. On reconnect, translate each intention into the existing database's transaction or API call, then project the authoritative result back into Jazz.

The authoritative database still owns business rules and final write ordering. Jazz adds local availability, reactive transport, shared caching, and durable offline intentions without requiring that database to become local-first itself.

>[!NOTE]
>By using Jazz as a layer in front of a traditional database, you not only unlock offline/local-first capabilities, you also add a powerful relation-based authorisation engine, which you can use to create more complex access criteria than your existing database allows.

## Run

```sh
cp .env.example .env
pnpm install
pnpm dev
```

Open `http://127.0.0.1:5173`. OAuth uses ATProto's loopback client metadata and redirects through `http://127.0.0.1:3001`; use the same loopback address for the app so its session cookie is sent correctly.

Note that `JAZZ_APP_ID` and `VITE_JAZZ_APP_ID` must match.