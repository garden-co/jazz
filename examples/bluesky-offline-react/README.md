# Jazz ❤️ Bluesky — Local-first ATProto

A proof of concept showing how Jazz can be layered over ATProto. It provides a following timeline with text and image posts, lazy-loaded threads, likes, reposts, and queued offline writes, while the PDS remains authoritative.

- ATProto remains the source of truth for posts, likes, and reposts.
- A slim Hono server performs authenticated PDS writes.
- The BFF owns bounded head polling and the opaque AppView cursor. The client can only request the next semantic page with `POST /api/timeline/more`.
- React renders the timeline from one bounded, owner-scoped Jazz query. Required includes prevent partially available cards from rendering, then enter the subscription as Jazz receives their rows.
- Posts and reaction intentions are written to Jazz first. If the online PDS write fails, they remain queued and retry after connectivity returns.
- A service worker precaches the complete application shell, including Jazz's WASM and browser workers, so an installed production build can cold-start without a network connection.

## Data flow

```text
READ
Bluesky AppView ──20 events──> BFF coordinator ──> Jazz bridge ──rows──> Jazz ──useAll──> React

MORE
React ──POST /api/timeline/more──> BFF coordinator ──uses its cursor──> Bluesky AppView

WRITE
Bluesky PDS <──── Bluesky adapter <──── Jazz bridge <──── pending Jazz intentions <──── React
```

The BFF starts one bounded head poll per active account and coalesces concurrent work for that account. It alone owns AppView polling and pagination cursors. `POST /api/timeline/more` requests one bounded older page; its response contains only `count` and `hasMore`, never the cursor or ATProto records. If deduplication means that one page supplies fewer than twenty new roots, the client can repeat the same semantic command without learning how the BFF paginates.

Projected rows reach React only through Jazz. The bounded `useAll` query is scoped by the signed-in DID and uses required includes for each card's post, thread root, profile, and repost reason. A card enters the subscription when those required rows are locally available; images, reactions, and other optional relations can continue updating it progressively. The UI keeps the previous window visible until Jazz has supplied the next twenty roots, then reveals exactly twenty together.

> [!NOTE]
> The BFF's bounded head poll could be replaced with a firehose consumer. Jazz would still be the only data-delivery channel to React. Continuously consuming the firehose is substantially more resource-intensive, so it is deliberately outside this POC's scope.

## Architecture

The example keeps the boundary between the authoritative system and Jazz deliberately narrow:

| Component                              | Responsibility                                                                                | Knows about Jazz?            | Knows about Bluesky?                |
| -------------------------------------- | --------------------------------------------------------------------------------------------- | ---------------------------- | ----------------------------------- |
| `server/app.ts`                        | Session guard, HTTP validation, and error mapping                                             | Authentication only          | Only application-level route inputs |
| `server/auth.ts`                       | Compose ATProto OAuth, opaque BFF sessions, and Jazz JWTs                                     | Yes                          | Yes                                 |
| `server/oauth-session-store.ts`        | Encrypt and persist authentication material in a backend-only Jazz table                      | Yes                          | No                                  |
| `server/signing-keys.ts`               | Persist the stable ES256 key used to sign Jazz JWTs                                           | Through its encrypted store  | No                                  |
| `server/jazz.ts`                       | Shared server-side Jazz context                                                               | Yes                          | No                                  |
| `server/bluesky.ts`                    | Read from AppView; write to the PDS                                                           | No                           | Yes                                 |
| `server/bridge.ts`                     | Own head polling and AppView cursors, fetch authoritative reads, and apply ordered PDS writes | Through its projection       | Yes                                 |
| `server/projection.ts`                 | Turn ATProto views and reconciled intentions into typed, idempotent Jazz writes               | Yes                          | Yes                                 |
| `schema.ts`                            | Local relational projection and pending intentions                                            | Yes                          | No protocol calls                   |
| `permissions.ts`                       | Client access to projected rows and locally queued intentions                                 | Yes                          | No                                  |
| `shared/pending-operations.ts`         | Serialise and validate the offline-write contract                                             | Describes intention rows     | Describes source operations         |
| `src/Timeline.tsx`                     | Compose reactive Jazz data, connectivity, actions, and presentation                           | Through its data and actions | Only calls the thread trigger route |
| `src/model/timeline-data.ts`           | Define the bounded owner-scoped required-include query and turn its rows into display threads | Yes                          | No                                  |
| `src/hooks/use-timeline-actions.ts`    | Apply optimistic posts and reactions to Jazz before asking the outbox to reconcile them       | Yes                          | Through the outbox                  |
| `src/hooks/use-timeline-projection.ts` | Reveal cached roots and request the next semantic twenty-root window                          | No                           | Knows no AppView cursor             |
| `src/hooks/use-outbox.ts`              | Serialise retries of queued intentions                                                        | Yes                          | Calls the reconcile route           |
| `src/components/TimelineView.tsx`      | Presentational React components                                                               | No                           | No                                  |
| `vite/pwa.ts`                          | Generate the install manifest and service worker                                              | No                           | Keeps API traffic network-only      |

Tests live under `tests/`, grouped by the boundary they exercise: `server`, `client`, `shared`, or `tooling`. Client tests mirror the `components`, `model`, and `hooks` source folders, keeping runtime modules uncluttered and making each test's scope visible from its path.

The BFF exposes four application-level capabilities:

- Own the bounded head poll and progressively project fresh authoritative pages into Jazz.
- `POST /api/timeline/more`: project one bounded older page without exposing a cursor or records to React.
- `projectThread`: lazily read one thread and add it to the same projection.
- `reconcileOperations`: apply queued local intentions to the authoritative system, then update Jazz with the result.

For illustration purposes, Bluesky-specific OAuth, XRPC endpoints, AT URIs, records, and TIDs stay on one side. Jazz tables, permissions, reactive transport, and pending-operation rows stay on the other.

## State lifecycles

Timeline rows move from AppView through normalisation into idempotent Jazz upserts. The BFF retains head-poll and pagination state; React retains only its visible root count. The client never merges an HTTP timeline payload: its bounded owner-scoped `useAll` query renders cached rows immediately and reacts as Jazz transports newly projected rows.

Local posts and reactions are written to Jazz as `queued` intentions. The outbox serialises PDS writes and marks successful submissions as `sent`. For reactions, that durable marker remains until a later AppView read confirms the desired state; this means a process restart cannot let stale remote state overwrite the local intention. If several offline reaction changes target the same post, the final queued intention wins. This POC deliberately has no conflict UI.

Authentication has two storage classes:

- OAuth sessions, OAuth callback state, opaque BFF session mappings, and Jazz JWT signing keys are encrypted and persisted in the backend-only `oauthSessions` Jazz table.
- The browser caches the short-lived Jazz JWT in local storage so an already-open or previously opened page can use its local Jazz data while the BFF is unreachable. An authoritative `401` or `403` clears that cache; a network failure does not.

The login form state, in-flight HTTP requests, and projection-status history beyond the latest job are intentionally ephemeral.

## Offline installation

Production builds are installable PWAs. The small self-contained module in `vite/pwa.ts` emits the manifest and an isolated service worker; it has no application or Jazz dependencies. The worker precaches the HTML, JavaScript, CSS, Jazz WASM, and Jazz worker files. It deliberately does not intercept `/api/*`, `/xrpc/*`, or Jazz sync traffic: Jazz remains responsible for local rows and queued intentions, while network routes only request projection or reconciliation. The build produces one deployable `dist` directory, and the BFF serves both its API and the compiled frontend from one origin.

Bluesky avatars and post images are cached as they are viewed. This media cache keeps at most 100 responses and expires entries after seven days so images cannot grow without bound. Text, profiles, thread structure, reactions, and the outbox remain in Jazz rather than the service-worker cache.

The service worker is registered only in production builds, avoiding stale development modules during Vite hot reloads. To inspect the complete production application locally:

```sh
pnpm build
WEB_ORIGIN=http://127.0.0.1:3001 pnpm start
```

Open <http://127.0.0.1:3001>. Development still uses separate Vite and BFF processes so Vite can provide hot-module replacement.

Loopback origins are considered secure for PWA development; a deployed copy must use HTTPS. A user must sign in and open the app online once before it can reopen their local timeline offline. OAuth and data that have never reached the device cannot work without a connection.

## Layering Jazz over another database

For a conventional SQL-backed application, keep the Jazz-facing projection and client pattern. Replace `server/bluesky.ts` with an adapter around the existing service or database, and replace the source-specific mapping in `server/projection.ts` and `server/bridge.ts`:

1. Let the server own bounded head polling and the existing database's cursor or primary-key position.
2. Normalise those records into the projection rows expected by the bridge.
3. Upsert them into Jazz with deterministic IDs so repeated reads and multiple users deduplicate.
4. Let clients render one bounded, user-scoped reactive Jazz query; do not merge an HTTP response into separate client state.
5. Represent offline writes as intention rows in Jazz.
6. On reconnect, translate each intention into the existing database's transaction or API call, then project the authoritative result back into Jazz.

The authoritative database still owns business rules and final write ordering. Jazz adds local availability, reactive transport, shared caching, and durable offline intentions without requiring that database to become local-first itself.

> [!NOTE]
> With ATProto, the PDS remains authoritative and Jazz permissions govern only the projected data delivered to clients. When Jazz fronts a traditional database, an application could instead choose Jazz as its client-facing authorisation layer, while server-side writes still enforce the rules required by the underlying system.

## POC limitations

- Timeline updates use BFF-owned bounded polling rather than consuming the firehose.
- Moderation labels and most rich embeds are not projected; image embeds are supported.
- The example has no production Jazz row-retention policy.
- The browser may evict service-worker or Jazz storage under device pressure; the POC does not request persistent storage.
- Loopback OAuth and environment-managed encryption keys are development choices, not a production deployment design.

## Run

```sh
cp .env.example .env
openssl rand -hex 32
# Copy the output into OAUTH_SESSION_ENCRYPTION_KEY in .env.
openssl rand -hex 32
# Copy the output into BACKEND_SECRET in .env. The local Jazz server and BFF
# must use this same value so projection writes run with backend authority.
pnpm install
pnpm dev
```

Open `http://127.0.0.1:5173`. OAuth uses ATProto's loopback client metadata and redirects through `http://127.0.0.1:3001`; use the same loopback address for the app so its session cookie is sent correctly.

The browser, BFF and local Jazz server share the application ID exported from `shared/identifiers.ts`. OAuth session material is encrypted with AES-256-GCM before it is stored in Jazz's backend-only `oauthSessions` table. `OAUTH_SESSION_ENCRYPTION_KEY` must contain a 64-character hexadecimal key; changing it invalidates existing sessions. The table has no client permissions and does not sync to browsers.

## Changing the schema

Keep existing local caches and describe schema changes with Jazz migrations:

```sh
pnpm exec jazz-tools migrations create --name "describe the change"
```

Review the generated snapshot and typed migration in `migrations/`. Fill in any explicit defaults requested by the generator, then run:

```sh
pnpm typecheck
pnpm test
pnpm build
```

Do not change the app ID or database path merely to bypass an incompatible schema. A new app ID creates a separate Jazz application and is useful only when isolation is the intended result. For normal development, keep the shared `jazzAppId` value and add a migration so existing local and server data can evolve.
