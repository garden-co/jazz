# Jazz ❤️ Bluesky — Local-first ATProto

A proof of concept showing how Jazz can be layered over ATProto. It provides a following timeline with text and image posts, lazy-loaded threads, likes, reposts, and queued offline writes, while the PDS remains authoritative.

- ATProto remains the source of truth for posts, likes, and reposts.
- A slim Hono server performs authenticated PDS writes.
- `/api/timeline` triggers bounded AppView fetches and projects the results into Jazz.
- React renders the timeline from one deep reactive Jazz query covering timeline events, posts, profiles, images, threads, likes, and reposts.
- Posts and reaction intentions are written to Jazz first. If the online PDS write fails, they remain queued and retry after connectivity returns.

## Data flow

```text
READ
Bluesky AppView ──20 events──> Bluesky adapter ──> Jazz bridge ──rows──> Jazz ──useAll──> React

WRITE
Bluesky PDS <──── Bluesky adapter <──── Jazz bridge <──── pending Jazz intentions <──── React
```

`/api/timeline` is only a trigger. It waits for one bounded AppView page, then returns cursor and count metadata; it does not deliver the fetched ATProto data to React. The bridge projects the page into Jazz, and the resulting rows sync to the client through Jazz's normal reactive path.

> [!NOTE]
> Client polling could be replaced with a push mechanism. For example, a firehose consumer could project rows into Jazz, which would then sync through the reactive Jazz subscription without a separate client messaging channel. Continuously consuming the firehose is substantially more resource-intensive, so it is deliberately outside this POC's bounded scope.

## Architecture

The example keeps the boundary between the authoritative system and Jazz deliberately narrow:

| Component | Responsibility | Knows about Jazz? | Knows about Bluesky? |
| --- | --- | --- | --- |
| `server/app.ts` | HTTP routes and request validation | No | Only session-shaped route inputs |
| `server/auth.ts` | OAuth sessions and Jazz JWTs | Stores sessions in a backend-only table | Yes |
| `server/jazz.ts` | Shared server-side Jazz context | Yes | No |
| `server/bluesky.ts` | Read from AppView; write to the PDS | No | Yes |
| `server/timeline.ts` | Pure Bluesky-to-projection normalisation | Only the projection shape | Yes |
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

> [!NOTE]
> With ATProto, the PDS remains authoritative and Jazz permissions govern only the projected data delivered to clients. When Jazz fronts a traditional database, an application could instead choose Jazz as its client-facing authorisation layer, while server-side writes still enforce the rules required by the underlying system.

## POC limitations

- Timeline updates use bounded polling rather than consuming the firehose.
- Moderation labels and most rich embeds are not projected; image embeds are supported.
- The example has no production cache-retention policy.
- Loopback OAuth and environment-managed encryption keys are development choices, not a production deployment design.

## Run

```sh
cp .env.example .env
openssl rand -hex 32
# Copy the output into OAUTH_SESSION_ENCRYPTION_KEY in .env.
pnpm install
pnpm dev
```

Open `http://127.0.0.1:5173`. OAuth uses ATProto's loopback client metadata and redirects through `http://127.0.0.1:3001`; use the same loopback address for the app so its session cookie is sent correctly.

Note that `JAZZ_APP_ID` and `VITE_JAZZ_APP_ID` must match. OAuth session material is encrypted with AES-256-GCM before it is stored in Jazz's backend-only `oauthSessions` table. `OAUTH_SESSION_ENCRYPTION_KEY` must contain a 64-character hexadecimal key; changing it invalidates existing sessions. The table has no client permissions and does not sync to browsers.
