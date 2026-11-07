# jazz-tools

## 2.0.0-alpha.60

### Major Changes

- 79b143e: Rename exclusive transactions to serializable transactions. `db.exclusiveTransaction(...)` is now `db.serializableTransaction(...)`, `db.beginExclusiveTransaction()` is now `db.beginSerializableTransaction()`, the Effect `jazz.exclusiveTransaction` is now `jazz.serializableTransaction`, `ExclusiveWriteResult` and `ExclusiveWriteHandle` are now `SerializableWriteResult` and `SerializableWriteHandle`, and the transaction kind `"exclusive"` is now `"serializable"`. A conflict rejection now has the code `"serialization_conflict"` instead of `"exclusive_conflict"`. The error for reads that are not supported yet now says "is not supported in serializable transactions yet". In Rust, `Db::exclusive_tx` is now `Db::serializable_tx`, and `ExclusiveTx`, `ExclusiveTxOps` and `ExclusiveTxRef` are now `SerializableTx`, `SerializableTxOps` and `SerializableTxRef`. The core Rust names follow: `TxKind::Exclusive` is now `TxKind::Serializable`, `RejectionReason::ExclusiveConflict` is now `RejectionReason::SerializationConflict`, `node::Error::UnsupportedExclusiveRead` is now `node::Error::UnsupportedSerializableRead`, `open_exclusive*` and `commit_exclusive*` are now `open_serializable*` and `commit_serializable*`, `Snapshot::exclusive_base` is now `Snapshot::serializable_base`, and `ViewCompleteExclusiveCoverage` is now `ViewCompleteSerializableCoverage`. The raw `writeState` JSON from the napi, wasm and React Native bindings now spells the conflict fate `SerializationConflict` instead of `ExclusiveConflict`. Only the names changed: validation, conflicts, and stored and synced data are the same, and the old names have no alias.

### Minor Changes

- c6cf029: Breaking: a client session takes no `appId`, and passing one now throws. The app id belongs to your backend's store (`remote({ appId })`, or the id a `sqlite()`, `memory()` or `vercel()` store keeps), and a browser or app client is scoped by its `serverUrl`. `createJazzClient`, `createJazzApp`, the framework providers (including Vue's `JazzProvider`, which drops its `appId` prop) and the React Native and Expo sessions now refuse one with an error that says where it goes. An app whose local-first users were created under an app id keeps them with `scope: "<that app id>"`; `useLocalFirstAuth` takes `scope` in place of `appId` too. The hero examples read `JAZZ_APP_ID` on the server only, and the examples E2E gate sets that instead of a public `<prefix>JAZZ_APP_ID`.
- 869a0da: Add optional `dependsOn` declarations to column transforms. Read callbacks receive stored dependency values; write callbacks receive app-facing dependency values, with missing values fetched automatically. Selections keep their public shape, and subscriptions recompute when dependencies change.

  Transform callback or dependency lookup failures prevent an explicit transaction from committing, even if the application catches the error. A caught synchronous input validation error before staging does not, by itself, invalidate the transaction.

- 1e46bc8: `createJazz` refuses local-first guests unless your `authenticate` lets them in. Return `acceptLocalFirstAuth(request)` (from `jazz-tools/backend`) for the signed-out requests you want guests to make:

  ```ts
  authenticate: async (request) => {
    const session = await auth.api.getSession({ headers: request.headers });
    return session ? { userId: session.user.id } : acceptLocalFirstAuth(request);
  },
  ```

  An app with no sign-in, where every browser is a guest, passes `authenticate: acceptLocalFirstAuth`. A refused guest's sync session fails as an auth failure, and its data stays on the device. An open guest session ends once `authenticate` stops accepting guests, at the next `recheckSeconds` check. A request that `authenticate` names as a user is that user's, never a guest's. `startTestJazz` keeps letting guests in; pass `guests: false` to test the refusal.

- 5d3b5cf: Backend writes no longer create accounts implicitly. A user gets an account from a client sign-in, a guest link, or the new `jazz.createAccountFor(userId)`. `forReqUser` and `withAttribution` throw `NoJazzAccountError` (code `no_account`) for a user without one, and `accountFor` returns null. In Effect, `JazzBackend.forReqUser` / `withAttribution` fail with `JazzNoAccount`, and `JazzBackend` gains `createAccountFor`. A browser whose remembered user has lost their account on the core (a reset dev store) signs them in again instead of retrying sync forever.
- b4f6576: Add `s.computed(callback)` alongside relationships in `s.table`'s second argument for virtual, read-only columns with inferred row types and automatically discovered dependencies. Computed fields use app-facing values, work in selected reads, includes, relationship hops, transactions and subscriptions, and add no stored columns. Existing `.transform({ from, to })` columns remain stored and writable.

### Patch Changes

- 06f7918: A client scoped to an alpha.59 app (`createJazzClient({ scope: "<alpha.59 app id>" })`, or `<JazzProvider scope="…">`) now finds each local-first guest's alpha.59 account on its device by itself, so guests keep their accounts after the upgrade. The alpha.59 entry is left in place, so a rollback still finds it.
- ac1e701: Self-hosted alpha.59 servers can move their data to this release. Download `jazz-alpha59-alpha60-export-<platform>` and `jazz-alpha59-alpha60-import-<platform>` (Linux x64/arm64, macOS arm64) from this release's GitHub assets, checked against `jazz-alpha59-alpha60-export.sha256` and `jazz-alpha59-alpha60-import.sha256`, and export the stopped server's data dir with the exporter, then run `jazz-alpha59-alpha60-import import --dump <dump> --data-dir <new dir> --app-id <app id>` and `jazz-alpha59-alpha60-import verify` with the same flags. The import keeps every row with its authors and timestamps, the accounts, and the active schema and permissions, and links each user's existing identity to the `createJazz` identity their backend will use, so returning users keep their accounts. Permissions that use alpha.59's `session.user.account` and `session.user.identity` come over as `session.user` and `session.identity`. `--app-id` must be the app id the alpha.59 server ran as, spelled exactly as it was.
- 6828000: The Jazz server now reads the query indexes of stored tables from storage by key and keeps only a bounded cache of them in memory, instead of holding every indexed row. The cache's budget is a quarter of the container memory limit (or `JAZZ_QUERY_CACHE_BUDGET_BYTES`). When resident memory passes 80% of the limit, the budget steps down gradually, and queries read more rows from storage. It stops lowering when that does not free memory, and grows back once memory frees up. Set `JAZZ_STORED_ARRANGEMENTS=0` to hold the indexes in memory as before. Clients are unchanged and keep holding their indexes.
- 4eb7492: `jazz.attach(server)` no longer opens the store, so a main backend whose `remote()` core is down at start keeps running instead of crashing on a bare "fetch failed". Until the core is reachable, Jazz routes and sync sockets answer 503, the server log names the core's URL, and the next use after the backoff tries again. `attach()` takes the server types Hono's `serve()` returns (an HTTP/2 server without HTTP/1 is refused with an error, since it has no WebSocket upgrades), and Jazz's sync sockets no longer reach the server's other upgrade listeners, which kept a custom Next.js server from syncing over WebSockets.
- 1c9f250: Auth state now reports a revoked account identity as its own `"revoked"` failure reason. Accounts no longer try to renew a revoked identity; they stay unauthenticated so the app can sign the user out.
- d0262b9: A client whose server's store was recreated (a deleted dev data directory, a restarted in-memory server, a new environment behind the same URL) no longer stays silently stuck. The core refuses that server with a typed `authority_changed` error: `db.getSyncStatus().link` becomes `{ state: "offline", reason: "authority-changed" }`, remote subscriptions end with it, and `wait({ tier: "global" })` and `waitForSync()` fail fast with an `AuthorityChangedError` (detect it with `isAuthorityChangedError`) instead of hanging. Offline writes are kept locally and are no longer uploaded into the new server's history. Nothing is reset automatically: call `db.deleteClientStorage()` (or delete the client's data directory in Node) to sync with the new server. Servers now mint their authority namespace when the store is opened, so a backup taken before any client connected restores to the same authority.

  Any upstream link the core refuses now stops sync instead of keeping a socket open that nothing syncs over: besides a changed authority, this covers a stranded store and a server catalogue this client cannot mirror (for example a column type mismatch). Those show as `{ state: "offline", reason: "stopped" }` and are retried only by `db.reconnect()`.

  A client that reconnects with unsynced writes now uploads them after the server's welcome instead of with its join, which costs one extra round trip.

  This is verified for clients that hold the server link themselves (Node and the core). In the browser, where tabs sync through a shared worker, the tab path is unverified: a tab's `global` waits are not yet told about a refusal its worker received and may keep waiting.

  Rust: `jazz::db::ErrorCode` has a new variant, `AuthorityChanged`, so exhaustive `match`es on `ErrorCode` need a new arm.

- fbab0f6: `createJazz` with a `remote()` store now waits for a hibernated Jazz Cloud app to wake instead of failing its start with `Backend admission failed (503)` or a timeout. Admission retries gateway and not-ready answers (`502`, `503`, `504`), network failures and stalled attempts with jittered backoff, honouring `Retry-After`, for up to three minutes (the cloud gateway's own hold); any other answer, such as `401` for a wrong secret, still fails at once, as does a malformed server URL or secret. Backend clients that connect to your main backend with a `credential` wait the same way.
- 224fae1: A backend client's `session.close()` (from `createJazzClient` in `jazz-tools/backend`) now uploads the writes it holds before it closes. Before, a script that inserted rows and then closed exited cleanly with none of them on the server. It waits up to 10 seconds, or as long as `close({ timeoutMs })` says; `timeoutMs: 0` closes at once. It stops waiting early when the server refused this client or it is offline for good. Writes still not uploaded then are handled by the driver: a `persistent` driver keeps them and uploads them when the client opens again, so `close()` only warns. With the `memory` driver they are lost, so `close()` still closes, then rejects with an error that says how many writes were lost and why (with `timeoutMs: 0` it warns instead). Writes the server rejects are reported as before. `process.exit()` with writes still pending still loses them, so close the session or await `db.waitForSync()` first.
- f4d0340: A trusted backend client can act for a user: `client.forUser(userId)` on a `createJazzClient` from `jazz-tools/backend`, opened with a credential the main backend trusts as `"backend"`, returns the database as that user, like the main backend's `jazz.forReqUser(request)`. Writes carry the user's identity and are checked against their permissions. A user with no account yet gets a `NoJazzAccountError`.
- 2b457ce: createJazz: backend clients. A machine outside your main backend can sync through its `/jazz` with its platform's OIDC token. List each trusted workload in `backendClients` with `trustWorkload({ issuer, audience, claims, as })`. With `as: "backend"` it is a trusted machine and syncs with the main backend's authority. With a function from the token's claims to a user it is a scoped machine and acts as that user, with that user's permissions. In Node, pass the token, or a function that returns a fresh one, as `createJazzClient({ credential })`: the session opens as whatever the main backend trusts the token as. createJazz no longer forwards a request's `Authorization` header to the core.
- 4d0e355: **Breaking:** your app's backend runs Jazz with `createJazz`, which holds the core's secrets and asks your `authenticate` who each request is. The Node path that connected to a core directly with its backend secret is gone:
  - **`createJazzClient` from `jazz-tools/backend`** refuses `initial: { backendSecret }`, pointing to `createJazz`. It stays for backend clients, which connect to your main backend with a `credential` (see Backend clients). The browser and React Native `createJazzClient` no longer accept a backend secret as `initial` either; `initial` is `"local-first"` or `"signed-out"`.
  - **External JWT verification in Node.** `createJazzClient` no longer takes `jwksUrl`, `jwtPublicKey`, `jwtIssuer` or `jwtAudience`, and refuses them as not supported; the `BackendJwtPublicKey` type and `resolveRequestSession` are no longer exported. Verify your identity provider's tokens in `createJazz`'s `authenticate` with `fromJWT({ jwksUrl, issuer, audience })`, and `jazz.forReqUser(request)` acts as the user it names. Local-first guests' bearer tokens are still verified.
  - **`jazz-tools/effect/backend`.** `JazzBackend.layer` takes `createJazz`'s options (or use `JazzBackend.layerJazz(jazz)` for a Jazz you already created); `forReqUser` and `forCurrentReqUser` act as the user your `authenticate` names. `JazzBackend.forAccount` is gone.

    The rest of the Effect backend service changed with it: `JazzBackendService.session`, `.client` and `withAttributionForRequest` are removed; `withAttribution` takes a `Request`; `JazzBackend.fromSession`/`layerSession` are now `fromJazz`/`layerJazz`; `forReqUser` and `forCurrentReqUser` take only a `Request` (no `RequestLike` or options); `BackendRequestOptions` and `BackendRequestAuthConfig` are no longer exported.

  - **`becomeBackend` is removed** from sessions (all frameworks' session actions), from `AccountManager`, and `"becomeBackend"` from `JazzSessionOperation` and `AccountOperation`. A process becomes your app's backend only by running `createJazz`.

- a57a924: `remote()` takes one secret: `remote({ url, secret })`, the app's admin secret, replaces `remote({ url, adminSecret, backendSecret })`. Or pass `remote({ url, backendCredential })`, a function `createJazz` calls for each request and socket, such as one that returns your platform's OIDC token for a sync server started with `--trusted-workload-*`. The backend presents the credential in `X-Jazz-Backend-Credential` on every request and WebSocket upgrade to the core and states the access it needs in `X-Jazz-Backend-Access`. A browser's sync session reaches the core only after the backend admits its prelude, so a guest's session never gets more than its own token grants.

  jazz-tools server: a server with an admin secret also accepts it as `X-Jazz-Backend-Credential`, with the access stated in `X-Jazz-Backend-Access` (`backend` or `admin`). Secrets a request or prelude offers itself don't count next to a credential. Requests without a credential work as before. Backend access through the credential works even on a server with no backend secret configured.

  Upgrade the sync server first. A backend on this jazz-tools version presents the credential on every request, and a sync server older than this release refuses those requests. Jazz Cloud is upgraded before this jazz-tools release ships.

- 7b4967f: A backend's own `jazz.db` no longer loses its link when the backend does a burst of synchronous work right after the link opens. Inserting 20,000 rows in a loop held the event loop for several seconds, so the link's handshake missed the server's 2 s deadline and the first read after the inserts failed with "websocket closed". The link from `jazz.db` to the server in the same process now has no handshake deadline; links from the network keep it.
- 02aee10: A session that follows the app's backend (`createJazzClient` in the browser, React Native and Expo, and `JazzProvider` with `serverUrl`) no longer reports `signed-out` before the backend has answered who is signed in. With no account on the device, it now waits for that first answer, so a returning user goes from loading straight to ready instead of briefly seeing the app's `signedOut` screen on every page load or app launch.
- 18a2880: Keep a browser or native session's published states consistent while it follows your backend. A device now remembers the user your backend signed in (their account, identity and claims, never a credential) and reopens them on reload, offline too, instead of starting a fresh guest with `initial: "local-first"` each time; their claims refresh when the backend's first answer differs. Switching from one backend user to another is one transition, with no `signed-out` in between. A sign-out confirmation only signs out the account the check started with, so an app that continued as a guest meanwhile keeps it. A 404 from the backend-user route is retried with backoff and stops the watch only when it persists, and `reauthenticate()` rejects instead of resolving once the watch has stopped, including after the session's `close()`. With `clearOnSignOut` the device never remembers the backend user, so a shared device doesn't reopen the last user's account while the backend is unreachable.
- 0172c08: A backend now stops on SIGTERM or SIGINT while browsers are connected. Hosts shut down by closing their server and waiting for the connections it still serves (`next start`, an Effect runtime, `server.close()` in your own handler), and Jazz's sync sockets and HTTP streams stayed open until the browsers left: the old process lingered, kept its `sqlite()` store locked, and the restarted one refused to open it. On those signals every open `createJazz` now closes, ending its clients' sockets and streams with "server shutting down" so they reconnect to the next instance. When nothing else handles the signal, the process then stops as it would have; otherwise your handler decides. `jazz.close()` now also ends HTTP stream responses and accepted sockets itself, instead of leaving them to the client.
- 77ff655: Clearer backend store docs and errors, from adopter feedback. `vercel()` now says first that it is meant for dev too (outside a deployment it runs `local`, by default `sqlite({ path: ".jazz/local" })`); `sqlite()` states its one-process lock (`<path>/jazz.lock`) and its refusal on serverless hosts; and `Jazz.next()` shows the catch-all route file. On Vercel the serverless refusal points to `vercel()`.
- 697d44e: A chunk I/O batch that fetched the wrong object for a node now counts that fetch as failed, so the host backs off as it does for the fetch lane.
- 56c8214: A blob dir is claimed exclusively by one data dir (also on file systems without hard links), blob dir paths resolve symlinks, and `jazz-tools server` locks its data dir with the same lock embedded cores take.
- b1f61ba: Reads of large values in a network blob store (S3 or Vercel Blob) no longer wait behind uploads. The leaves a read needs are fetched beside the upload batch that is out, so a read no longer waits for megabytes of uploads to land, or for the backoff after a failed put. Each leaf is fetched only once, even while an upload batch and the fetches beside it are both out, and when fetches fail they back off as uploads do, while uploads carry on.
- 1cbfaa6: A blob store that fails no longer leaves clients waiting: subscriptions that need its large values are refused with the store's error, and a client's request for large-value chunks is answered once the store answers again, on the same connection. An S3 read denied with AccessDenied is now a storage error that names the missing permission, instead of being reported as data loss, and a large value whose leaf went missing is read again as soon as the leaf is back, with no restart.
- 78a6ac5: A `vercel()` core claims its Blob store or S3 bucket for its Vercel environment, and a core of another environment is refused there: connect a separate store to production and to previews. The server takes the environment with `--blobs-environment`.
- 4575633: Fix branch-view reads so a head row that an older schema cannot show still hides the base row, and so tables whose branch column is an enum read back their branch values instead of failing with a type error.
- c4d4f0e: Keep branch-view subscriptions in their view across sync. A client's live subscription through a branch view now gets that view's own result from the server, its limit and ordering included, instead of every branch's matching rows. A view over a frozen base keeps its head live from the server. Writes to branches a view does not read no longer re-run its subscriptions. The server also sends the draft's version of each base row the view's query matches, so rows the draft changed or deleted are masked even when another read brought their base version. When the draft changes a row so that you may no longer read it, the row is hidden in the draft view instead of showing the main version it replaced, and it reappears once you may read the draft's version again.

  Each existing live branch-view subscription opens fresh once after upgrading, as its stored resume point is now keyed by its view too.

- 08d0147: Browser clients no longer name an app id. A client's local data, local-first secret, guest proofs and founder account are scoped by the endpoint it syncs with, and `serverUrl` defaults to `"/jazz"`, the app's own backend. The backend checks a guest's proof against its own endpoint URL and vouches for it to the core, so `appId` now lives only in `createJazz` and `remote()`.

  Upgrading: a browser that drops `appId` without passing `scope` starts with a new local-first identity and a new local database. An app with existing local-first users replaces `appId: "…"` with `scope: "…"` (the same value) to keep their local-first identities and accounts. Local stores written by alpha.59 still have to be deleted and resynced (see the upgrading-from-alpha-59 note). Behind a proxy that changes the host or path browsers use, set `createJazz({ publicUrl })` to that URL.

- 3cd02fd: Detect unresponsive browser storage workers while mutations await local durability or when a page resumes, and automatically replace failed worker connections while preserving queued transactions. Recovery works offline and retains the original write handles.
- f69a08a: A `schema.ts` and `permissions.ts` that import each other no longer fail `jazz-tools validate` and `migrations create` with a bare "Cannot convert undefined or null to object". `definePermissions` says the import is circular and how to break it, and a schema that fails to load names the file being loaded.
- 7089d18: `createJazz`'s `recheckSeconds` now compares a signed-in user's claims too, not just their user id. When `authenticate` names the same user with different claims (a role changed), their open sync sessions end with "authenticate again" and the client reconnects with the new claims, so policies stop applying the old ones within `recheckSeconds` (60 s by default) rather than at `sessionTtlSeconds` (an hour). Claims compare as JSON with object keys sorted; return claims that only change when the user's access does.
- cf74001: With `clearOnSignOut`, a guest who signs up no longer leaves their guest signing root on the device. Before, the root stayed in browser storage after the sign-up and the sign-out, so the next person on a shared device could reopen that account with `restoreLocalFirst` and see the user's data. Jazz now forgets a guest's root as soon as the backend links it to a user, since the backend signs that user in from then on. The account store also keeps a short list of the accounts it forgot (account ids, never keys), so another tab that still had the guest open cannot write the root back. Restoring the root on purpose with `restoreLocalFirst` takes it off that list.

  A guest who never signs up keeps their root, which is the only key to their account. So does a device without `clearOnSignOut`. A copy of the root the app saved itself, for example from `exportLocalFirstSecret`, is the app's to delete. Tabs running an older Jazz version don't know the list: their next write drops it, after which they, and tabs on this version that still hold the root, can write it back until they reload. `guestKeyAfterLink: "revoke"` on the backend still also retires the key on the server.

- c95c1f6: Node, Deno, Bun and CLI hosts can sign in through the app's backend with the app's own token: pass `bearer` to `createJazzClient` (or `createAccountManager`) from `jazz-tools/client`. It is sent as `Authorization: Bearer` on sign-in, on Jazz's checks of who is signed in, and on every sync connection, thin and local-first, so the backend's `authenticate` reads it there. Not supported yet in a browser, where the backend's cookie names the user.
- a6a6bb4: Report a write the server refuses because the client clock is too far ahead with the `client_clock_too_far_ahead` rejection code instead of the generic `write_rejected`.
- ced6c4c: Browser and React Native reads that name no tier now wait up to 5 s for the server on their first load while online (`firstLoadRemoteWaitMs: 5000`), so a one-shot read no longer returns a stale cache. Pass `tier: "local-first"` or `firstLoadRemoteWaitMs: 0` to show local data at once. Servers and Node still default to `"remote"`.
- a64a9fe: `createJazzClient({ fetch })` opens again: the sign-in `fetch` (and the account `store`) stay with the account manager instead of reaching the client's configuration, where `db.getConfig()` threw `DataCloneError`.
- 836ab14: Browser and React Native clients follow the user your backend names: your backend's `authenticate` is the only source of who is signed in. Jazz asks your backend on start, when the page or app returns to the foreground and when another tab signs in or out; there a signed-out answer signs out only once a second answer agrees. After your own sign-in, sign-up, sign-out or account switch, call `reauthenticate()` (from `useJazzAuth()`, on a session, or on `createJazzApp`'s app) once your session cookie or bearer is fresh: Jazz asks your backend and follows its answer at once. `useJazzAuth()` exposes that answer as `user` (`{ id, issuer, claims }`, or `null` for a guest or no one). **Breaking:** the client no longer takes the user from the app: the providers' `user` and `auth` props, `backendAuth()`, `betterAuth()`, `authForUser()`, `JazzAuth`, `setUser()`, `connectAuthProvider()`, `connectBetterAuth()`, `useBetterAuth()` and `useAuthProvider()` are removed; call `reauthenticate()` instead. It rejects when your backend can't be asked or following it fails, and retries a session left in error for the same user. `user.claims` are as of sign-in: reauthenticating as the same user doesn't update them. Apps start signed out until your backend names a user; local-first guests are opt-in with `initial="local-first"` (previously the providers' default). Signing out, or switching to another user, closes that account's client; in browsers, `clearOnSignOut` wipes its local storage for shared devices, after waiting up to 5 seconds for pending writes to upload (not yet supported in React Native). `createJazz` re-runs `authenticate` every `recheckSeconds` (default 60) and ends a sync connection whose user it no longer names. The client reconnects with fresh cookies; if no one is signed in, that reconnect fails as an auth failure and the client stops retrying, reconnecting with backoff while the backend still names the same user. A refusal alone never signs anyone out: that waits for a check after focus, or 30 seconds later on a visible page.
- 4753e73: **Breaking:** clients no longer sign in to Jazz with a JWT. Sign-in goes through the app's backend: `createJazz({ authenticate })` reads the app's own session, and the framework bindings follow the user it names (call `reauthenticate()` after your own sign-in or sign-out). A local-first guest keeps its data by calling `accounts.linkWithBackend()` after signing in. The `jwtAuth` helpers are removed, and `registerJWT`, `loginJWT` and `loginOrRegisterJWT` are internal (test fixtures only) and no longer supported for apps. The sync server verifies no user JWTs either (see the core-admits-only-its-backend changeset).

  ### Migrating an app that signs in with a JWT
  1. Add `createJazz` to your backend (`src/lib/jazz.server.ts`) with your schema, permissions and store (`remote({ url, appId, secret })` with your app id and its admin secret to keep using Jazz Cloud or your own sync server), and an `authenticate(request)` that reads your auth library's session and returns `{ userId, claims }`.
  2. Mount it at `/jazz` (Next.js: `app/jazz/[...path]/route.ts` exporting `GET`, `POST` and `DELETE` from `jazz.next()`; Hono and other fetch hosts: `jazz.handle`; Node: `nodeHandler` + `attach`).
  3. In the browser, set `serverUrl="/jazz"`, remove the JWT auth, and call `reauthenticate()` from `useJazzAuth()` after your auth library signs in or out (apps start signed out; pass `initial="local-first"` for local-first guests). Remove the code that minted or fetched Jazz JWTs, and the sync server's JWKS settings.
  4. Keep `JAZZ_ADMIN_SECRET` on the server only; the backend no longer needs `BACKEND_SECRET`.
     **Returning JWT users:** their accounts belong to the identity (your JWT's issuer, its `sub`), while `createJazz` names a user `jazz:backend` with your `userId`. The alpha.59 import (`jazz-alpha59-alpha60-import import`, a binary on the release's GitHub page) links the two on the same account by default, so an `authenticate` that returns the old `sub` as `userId` (as `fromJWT()` does) opens the user's old account. Local-first guests are not affected.
  5. A page served from another origin than the backend must be listed in `createJazz({ allowedOrigins })`.

- e7fefc5: `next start` now stops on SIGTERM while browsers are connected. Next waits for its keep-alive connections, and a browser re-opening its sync stream to the closed Jazz kept one busy, so the process stayed up with its port bound and logged "this Jazz was closed" on every retry. Once a Jazz has closed, its routes and sync sockets answer 503 with `Connection: close` (on HTTP/1; HTTP/2 has no such header) and log nothing, so the connection ends and the browser reconnects to the next instance. A store that can't open keeps its connections.
- e7e7263: A server keeps the sessions it ends, with why, only for a host that tracks them, so a host that never takes them no longer accumulates them.
- 221c524: Add `jazz-tools login`, `logout`, `whoami`, `teams` and `apps` commands to manage Jazz Cloud with your dashboard account (WorkOS device login and short-lived tokens) instead of per-app admin secrets. Every command supports `--json`.
- ef60e40: Keep a new Cloud CLI login intact when an older logout finishes. Logout now removes and revokes the same captured session, including when its access token needs refreshing.
- 0aec19c: Fix live combined aggregate subscriptions missing changes when the first aggregate value stays unchanged. Complete summaries now identify replacements without changing group occurrences or limiting large multi-output summaries.
- cdaaf4d: Every sync upstream now tells its clients the position the whole connection is settled through, in a new `ConnectionSettled` message after a tick's rows, settles and fates. A read's settle speaks for that read alone, so an upstream may settle reads at different positions; what a client knows of its upstream moves only by the connection's position. This prepares the client for sharded upstreams and changes nothing a client reads from an unsharded server.
- 4a3c64c: `loginWithBackend()` and `linkWithBackend()` now refuse outside a browser page or worker (Node, Deno, a CLI), on `jazz-tools/client`, `jazz-tools/thin` and the root `createAccountManager`, with an error that says bearer sign-in is supported on React Native (`bearer` on `createAccountManager` from `jazz-tools/react-native` or `jazz-tools/expo`). Before, a bearer added through the `fetch` option signed the user in, but the sync connection carried no identity: the session reported ready, global waits never settled, and "Account auth refresh failed" was logged every few seconds. A user the backend signed in no longer runs that token refresh at all, so a browser whose backend session lapses stops logging it too.
- c04380b: **Breaking:** a sync server (core) admits only its app's backend. Browsers, React Native apps and other clients sync through the app's `createJazz` handler (`serverUrl: "/jazz"` or the backend's `/jazz` URL); a client that connects straight to a core is refused with "this core admits only its app's backend". Local-first guests keep working: the backend relays their proofs. Removed with it:
  - **Server JWT verification.** `jazz-tools server` no longer takes `--jwks-url`, `--jwt-public-key`, `--jwt-issuer`, `--jwt-audience`, `--auth-cookie-name` or `--trust-proxy`, and refuses to start while their `JAZZ_*` environment variables are set, naming them. `JazzServer.start()` (jazz-napi) rejects `jwksUrl`, `jwtIssuer` and `jwtAudience` (and any other unknown option) instead of ignoring them; `JazzCore.open()` no longer takes `jwtPublicKey` and the other JWT options. `startLocalJazzServer()` drops its `jwksUrl`, `jwtIssuer` and `jwtAudience` options. The `JAZZ_JWKS_CACHE_TTL_SECS` and `JAZZ_JWKS_MAX_STALE_SECS` variables are gone. Trusted workloads (`--trusted-workload-*`) still verify their OIDC tokens.
  - **Public account routes.** `/apps/:id/accounts/register`, `login`, `login-or-register` and `found-local-first` are gone. The backend vouches for its users at `/apps/:id/admin/accounts/*` with its backend secret (or admin secret), which `createJazz` does for you.
  - **Test helpers.** jazz-napi no longer exports `TestJwtIssuer`. `startTestJwtIssuer().jwtForUser()` is now async and its default claims are `{}` (it was `{ role: "user" }`); pass the claims a test needs.
  - **Cloud CLI.** `jazz-tools apps auth set` takes only `--allow-local-first-auth`; `--jwks-url`, `--jwt-issuer`, `--jwt-audience`, `--jwt-public-key` and `--jwt-public-key-file` are refused as not supported.

  ### Migrating
  - A web or React Native app: add `createJazz` to your backend with `remote({ url, appId, secret })` (your sync server, your app id and its admin secret), mount it at `/jazz`, and point the client's `serverUrl` there. Sign users in with your backend's own session (`authenticate`), as the client-signs-in-through-backend changeset describes.
  - A Rust or other native service that held a user JWT: it is the app's backend now. Give it the core's backend secret and state the user it acts for (`AppContext { backend_secret, backend_session: Some(Session::new("jazz:backend", user_id)), .. }`), after resolving that user's account at `/admin/accounts/login-or-register`. `examples/todo-server-rs` shows this.
  - Drop `--jwks-url` and the other JWT settings from your sync server's command line and environment; it fails to start while they are set.

- fbbb242: Core errors from the WASM binding, NAPI promise rejections and React Native now carry their stable reason name as `error.reason` (for example `authorization_denied`), and rejected writes are classified by it instead of by their message. The `causality_violation` and `cascade_rejected` rejection codes, which the SQLite engine never produces, are gone, and the Better Auth adapter no longer retries `cascade_rejected`.
- 8a254de: Add a shared core mutation-admission fence so hosts can stop application writes
  while draining committed uploads, authority receipts and storage cleanup.
- 085d18d: A Jazz core now serves only sessions pinned to a published permissions bundle. Every user session it governs is governed by the bundle the app's backend pinned it to, never by permissions activated on the core, so a core refuses a stated session without a bundle even before the app published one. `createJazz` already pins every session it opens, so apps on `createJazz` need no change. The refusal arrives as a terminal auth failure (`AuthFailed`, no retry) instead of a retryable internal error, and so does a session pinned to a bundle the core never published (a mistyped or foreign bundle id), so the client reports it instead of retrying forever.

  Rust: `AppContext` gains `bundle`, the hex id of the published bundle a `backend_session` is pinned to; the client sends it in its socket prelude. A Rust backend that acts for a user sets it to the bundle the app published. A permissions change reaches a session when it reconnects pinned to the new bundle.

  Fixed: a subscription pinned to a bundle kept that bundle's permissions only until the schema changed; it was then re-run under the core's own permissions.

- 5577483: **Breaking:** `createJazzSession` is now `createJazzClient`, for frontend clients (browser, React Native, Expo) and backend clients alike, and `JazzSessionConfig` is now `JazzClientConfig`. The low-level `createJazzClient` that opened a client for an `AccountHandle` is no longer exported: `createJazzClient` (or your framework's provider) owns every client's account lifecycle.

  Everything left from connecting to a Jazz core directly is gone. Your backend runs Jazz with `createJazz`, which holds the core's secrets:
  - The Node client (`jazz-tools/backend`) has no `forReqUser`, `forAccount`, `withAttribution` or `withAttributionForRequest`. Use `jazz.forReqUser(request)` and `jazz.withAttribution(request)` on your `createJazz`, which ask your `authenticate` who the user is.
  - The Node client no longer takes `allowLocalFirstAuth` (it only governed those request scopes), and `BackendContextConfig` is no longer exported. A Jazz core's own `allowLocalFirstAuth` setting is unchanged.
  - `initial` on a Node client is `"local-first"` or left out.

- 78ff38d: createJazz no longer takes an `appId`. `remote({ url, appId, secret })` names the app the remote core serves. `sqlite()` generates an app id on first start and keeps it in `<path>/app-id`, `vercel()` keeps its id on the core's Drive, and `memory()` makes a fresh one. None of them takes an `appId`, and passing one throws: an alpha.59 app's id reaches a new store through `jazz-alpha59-alpha60-import import --app-id`, which writes it to `<path>/app-id`. `jazz.appId` is the id the core runs as. Passing `appId` to createJazz now fails with an error saying where it moved.
- 5818eec: New `db.isLocalFirst()` tells whether a `Db` reads and writes through a local store yet: true for a local-first `Db`, and for a thin one once the session has upgraded it in place.
- a92cebf: BREAKING CHANGE: `s.defineApp` and `s.defineSliceableApp` now take the app's migrations as a second argument, and throw without it. `jazz-tools validate` writes them to `migrations/index.ts` beside `schema.ts`, even while the app has one schema version. Clients use them to store every version exactly as the server does, so they no longer rename stored columns when they sync.

  To upgrade, run `jazz-tools validate`, then pass the generated module to your app:

  ```ts
  import { migrations } from "./migrations";

  export const app = s.defineApp(schema, { migrations });
  ```

  Commit `migrations/index.ts` with your schema. Apps that are never deployed, such as a throwaway test app, can pass `{ migrations: "none" }`. Tests that define their schemas in code can build the module with `appMigrations` from `jazz-tools/testing`. A client whose schema version is missing from its migrations refuses to open and tells you to regenerate them. Rust apps load the same module with `jazz::tools::deployment::published::client_migrations_module`.

- ab06af5: Add `$deletedAt` and `$deletedBy` as optional projection, filter, and sort columns after `.includeDeleted()`. They return the winning deletion's authored timestamp as a `Date` and author as a `RowAuthor`, or `null` for live and restored rows. History versions share the logical row's current deletion timestamp and author.

  Filters on `isNull`, `eq: null` and `ne: null` now become null checks for every column type before any type-specific handling. Author columns such as `$createdBy` previously threw on `eq: null`; they now match rows whose author is null.

  `$deletedAt` and `$deletedBy` are rejected in flat joins: `.includeDeleted()` widens only the root query, so joined sources have no deletion metadata.

  The query compiler now treats `$createdAt` and `$updatedAt` like other ordered scalar columns: a join on either can drive an indexed lookup, and an equality on either against a query parameter becomes a binding-route key instead of a residual predicate. Results are unchanged. Structured author columns stay predicates.

- a75a345: Deployments no longer activate permissions on the core. The `/admin/deploy` route is removed: `/admin/bundles` publishes a deployment's schemas, migrations and permissions as a bundle that sessions are pinned to, and makes nothing current. `deploy()` from `jazz-tools/dev` and `jazz-tools/testing` publishes the same way and returns the bundle's id as `bundle`. A new deployment's target no longer has to be reachable from an active schema; its history must still have a single root, so backends on sibling branches serve side by side.

  `GET /admin/bundles` lists every published bundle with its write schema and permissions, and `fetchPublishedBundles()` from `jazz-tools` reads it. The standalone Inspector shows the permissions of the latest bundle published for the schema it inspects.

  `createJazzClient()` from `jazz-tools/backend` takes `bundle`, the id `deploy()` returned. A backend that acts for its users without `createJazz` must pass it: an unpinned backend link can no longer act for a user, only as itself.

- 651b4a4: The store's own row columns now have descriptive names: `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched`, and `_partition` reserved for sharded deployments. An app column can no longer take any of these names.
- e4c338d: A server that cannot serve a message a client sent now ends that link with an error the client does not retry, instead of asking it to reconnect and resend the same message forever. A client waiting for a write to reach the server no longer spins while inbound frames wait for the next pump pass, which could starve its timers and exhaust the heap.
- 6b6a0f9: Name each new stored column after the schema version that introduces it, as `name~` and a short hash, instead of in the order a store admits versions. An offline client now stores its schema's new columns where the server does, even when the server admitted another version adding a column of the same name first, so its pending writes no longer have to be moved when it syncs.
- 5ed3cf6: `serverUrl: null` now keeps Jazz on this device only, as documented. It used to fall back to the same-origin `/jazz` endpoint. A device-only session opens the local-first client on a local-first account straight away, so it needs `client: () => import("jazz-tools/local-first")`. It never contacts a backend: `loginWithBackend()` and `linkWithBackend()` fail with a clear error, and a signed-out start is refused, because nothing could ever sign it in. The same holds in the React Native and Expo sessions.
- 662d9ed: Keep core ticks running while a foreground lease handoff drains started native work. The handoff closed the runtime before reading its transaction-time high-water, which stopped all core ticks; a native future that held the node lock until a tick completed its host continuation then never released it, so the handoff, and the client shutdown behind it, waited forever. Mutation admission still closes at the start of the handoff.
- f19f657: Browser account contexts now start their SharedWorker, and ask it to fetch, compile and instantiate its WASM, while the page is still loading its own runtime and resolving the account's credential, instead of only after both finish. Instantiation runs wasm-bindgen's start function, which only installs the panic hook. The worker still admits its durable owner before opening storage or configuring tracing, and an early lease is only adopted for the exact root it was acquired for; any other is returned before the replacement is acquired. URL-addressed WASM assets served as `application/wasm` are now compiled with `WebAssembly.compileStreaming` as they download. Any other content type, or a runtime without streaming compilation, falls back to the buffered load and its magic-byte check.
- 8bee608: An app on its own store (`createJazz` with `sqlite()`) can now follow the migrations docs without a Jazz server. The first `jazz-tools migrations create` with no snapshot and no `--server-url` snapshots `schema.ts` as the project's first version, as the docs describe. `migrations create` now also updates `migrations/index.ts`, so a new migration ships without another `validate`. `validate` says when `schema.ts` changed since the latest snapshot, instead of the permission-only note. The "disconnected roots" deploy error now says how to connect the versions.
- 3441d34: Add `createJazz` to `jazz-tools/backend`: Jazz runs inside your backend's process with `sqlite({ path })` or `memory()` (or in front of a sync server elsewhere, such as Jazz Cloud, with `remote({ url, secret })`, the app's admin secret), serves sync and sign-in same-origin under `/jazz`, and applies the schema, migrations and permissions from code when it opens, with no separate server or deploy step. Mount it with `jazz.handle`, `jazz.next()` (WebSocket on Vercel through `@vercel/functions`) or `jazz.nodeHandler` plus `jazz.attach` (`ws`). Browsers connect with `serverUrl: "/jazz"` and `session.loginWithBackend()`: the backend's `authenticate` names the user on sign-in and on every sync connection, so the browser holds no Jazz credential, and `jazz.linkIdentity` links a guest's or second sign-in method's identity to an existing account. Browsers fall back to an HTTP stream when a `createJazz` host can't upgrade sockets (it answers the socket path with 426); stream sessions live in the process that opened them, so they need a single backend instance or sticky routing. Requests from another origin than the backend's own, or one in `allowedOrigins`, are refused.

  **Breaking for custom auth integrations:** `JazzSessionActions` now requires `loginWithBackend`. The `auth` prop and its `JazzAuth` descriptors are removed from every `JazzProvider` (and `createJazzApp`): your backend's `authenticate` decides who is signed in, and the app calls `reauthenticate()` after its own sign-in or sign-out.

- 17747fc: Store enum columns, and arrays of them, as small integers that each store
  assigns locally. Reads, writes, filters and sync still use variant names.
  Sorting by an enum column now follows the schema's declaration order instead
  of alphabetical order, and adding or reordering variants makes a new schema
  version.

  Schema version ids now count enum variants, so every schema with an enum
  column gets a new id. Peers on earlier builds name those schemas by their
  old ids: in a mixed fleet using enums, clients and servers stop syncing
  (writes are refused as made under an undeployed schema version) until
  everyone upgrades.

- 9eafb84: Fix a client keeping rows after it lost read access to them. Two cases: a row the client wrote itself, when another client's write took its access in the same server tick, came back after the server evicted it; and rows the client held when it reconnected were not evicted when a write (its own offline write included) took its access while no live query listed them.
- a355b8e: `jazz-tools/expo`: `createJazzClient` and `<JazzProvider>` accept `bearer` in their types, like `jazz-tools/react-native`. It already reached the account manager at runtime, but TypeScript rejected it.
- 7ad9414: `jazz-tools/expo` exports `useSyncStatus` and `useAccountState` (and the `SyncStatus` and `LinkStatus` types), like `jazz-tools/react-native`.
- 7b07ed1: The Expo web target now fails with a clear error instead of `TypeError: Cannot read properties of undefined (reading 'get')`: `jazz-tools/react-native` and `jazz-tools/expo` run only in iOS and Android builds, and the error (`REACT_NATIVE_WEB_UNSUPPORTED_ERROR`) points web at the browser client from `jazz-tools/react`. `<JazzProvider>` and `<JazzSessionProvider config>` also log a startup failure with `console.error`, so an app without an `error` or `fallback` view no longer shows a blank screen with nothing in the console.
- 0a4f091: Faster sync: a server reads the rows it ships or drops, their history versions and flat-join rows in chunks instead of one statement per row. A client reads what it holds of a message's rows at once, and reuses each table's insert and author codes while it stores them. A server no longer ships stale columns after a table gains one.
- bc4200e: `jazz.forRequest(request)` is now `jazz.forReqUser(request)`: it hands back a `Db` that reads and writes as the request's user. In `jazz-tools/effect/backend`, `JazzBackend.forRequest` and `JazzBackend.forCurrentRequest` are now `JazzBackend.forReqUser` and `JazzBackend.forCurrentReqUser`. The old names are gone, with no alias.
- 77c8387: `fromJWT()` from `jazz-tools/backend` is a ready-made `authenticate` for `createJazz` that signs users in with JWTs from an identity provider (WorkOS, Auth0, Clerk, any OIDC issuer). It verifies the signature (`jwksUrl` or `publicKey`), `issuer`, `audience` (required; `false` opts out for issuers that are specific to one app) and expiry (`exp` is required; `exp` and `nbf` allow 60 seconds of clock skew, set by `clockToleranceSeconds`). It names the user by `sub` and passes the token's other claims to policies as `session.claims`. A request without a token, or with one that does not verify, is signed out.
- d6df812: `gather({ start })` now seeds the gather with only the rows `start` (or an earlier `where`) matches. Before, the seed filter was dropped and every row of the table seeded the gather, so `start` and `maxDepth` looked ignored.
- e9f7f16: A write's `wait({ tier: "global" })` and a serializable transaction's `wait()` now stay pending through network outages and reconnects instead of rejecting once the client reports the outage. They settle when the server acknowledges the write. They reject when the client stops trying to reach the server: a protocol error, closing the client, a disabled or revoked account, or any refused credential that nothing renews. An account's refresh cycle (and an owned local-first token) renews an expired credential, so its waits outlast the refresh; a client with a static token or a backend secret rejects them, and fails its remote subscriptions, as before. If an account's renewal never succeeds, its waits stay pending until a timeout or shutdown. Pass `timeoutMs` (up to 2147483647) to bound a wait: it then rejects with the new `WriteWaitTimeoutError` (`code: "timeout"`), and the write stays queued. React Native now treats a refused credential the same way as the web. Remote reads still fail during an outage.
- 9bc99be: A Global write wait in a browser tab running over its worker no longer rejects when the sync server refuses a renewable credential (an expired sign-in) or the link has an outage. It stays pending and resolves once the write reaches the server after re-login or reconnect, as documented. Before, it failed after about 60 ms with "session missing: not signed in" while the write stayed queued, so an app that retried wrote it twice. Remote reads still fail fast.
- ff0d69b: Fix grouped aggregate results using opaque identities for default ordering and explicit-order ties. Reads and live subscriptions now use logical group keys before pagination, while preserving group occurrence identities and explicit sort directions.
- b6aeaf2: A local-first guest who signs up keeps its data however the session notices the sign-in. Asking the backend who is signed in (`backend-user`) no longer creates the user's account, and when the user has none yet the session links the guest to them instead of signing them in. Before, the session's own check on start, on focus or after `reauthenticate()` gave the new user an account first, and the guest's `linkWithBackend()` was then refused for good.

  If the backend refuses that link, the session signs the user in to their own account instead and doesn't try the link again, and on shared devices `linkGuestOnSignUp: false` turns the automatic link off. Two tabs linking the same guest at once now both succeed when the backend revokes guest keys after a link.

- fd66e44: A local-first guest now syncs its writes even when its browser also sends a signed-in user's cookie. That happens when the user signs up and the guest reconnects before it links to them. Before, `createJazz` refused the guest's socket in that case, so linking failed or the guest's writes were lost.
- 9afb466: `StoreHeldByAnotherTabError` now says to close or reload the other tab, and that reloading the waiting tab before it joins loses its unsynced writes. Its old wording ("close or reload it") read as "reload this tab", which loses them. `session.restoreLocalFirst()` with a malformed secret (or none) now rejects without shutting the current client down, so the session goes on as it was instead of ending in the error state.
- d09159b: Query and subscribe to previous row versions through `app.$history.table` in TypeScript or `history.tableName` in Rust.

  History queries support filtering, projection, ordering, pagination, and local-first and remote reads and subscriptions, fetching missing history from upstream when needed. Historical content is transformed through schema lenses to the requested schema.

  Each version exposes `$acceptedAt` as a JavaScript `Date` and `$transactionId` as a `TxId`, alongside the usual opt-in attribution columns (`$createdAt`, `$createdBy`, `$updatedAt`, `$updatedBy`). Server authorization follows access to the current row.

  **Upgrade note:** history is available for every table, with no opt-in. Anyone who can currently read a row in a branch can read all retained versions of that row in that branch, including versions written while they had no read access. Review your read policies before upgrading if earlier versions of a row may hold content its current readers should not see.

  History tables are read-only and require a bounded `id` constraint. They cannot be filtered by `$transactionId` or used on either side of `.include()`, `.hopTo()`, or `.gather()`. Only accepted content versions appear: pending writes and deletion-only events do not add history rows. Deleted rows' content history can be queried with `.includeDeleted()`. History is currently indexed only by row ID.

  **Breaking wire change:** `SupportingRow` now includes an `is_current` flag, allowing queries to combine a current table and its history without treating historical versions as current rows.

  **Breaking wire and binding change:** `ResultKey` moves to version 2. A v2 key starts with byte `2`. After its union-arm records it carries one transaction flag byte per source occurrence: first the root, then each joined source in order. A flag of `1` is followed by that source version's `tx_time` (u64, big-endian) and `tx_node` (16 bytes); a flag of `0` marks a current source. Version 1 keys are rejected. `OutputOccurrenceId` now serializes as its `ResultKey` bytes instead of the old postcard `(root, joined)` pair, and the typed-row result member variant is removed. These keys are not persisted.

- 87b178a: Every server now numbers commit positions from a hybrid clock (wall-clock milliseconds plus a counter) instead of counting up by one, so an unsharded server runs the same code as one shard of a sharded deployment. Positions, write receipts' sequence numbers and `$transactionId` values are therefore large and not dense. They still only increase, and an existing store continues above what it numbered.
- bbfd7be: A Rust host can run a store's chunk-store I/O itself: with `DbConfig::with_host_chunk_io()`, `Db::tick` never waits on the chunk store, and the host takes each batch with `Db::take_chunk_io`, runs it with `ChunkIo::run` wherever it likes and applies the outcome with `Db::finish_chunk_io`. A network store then can't stall a synchronous tick.
- 3fb28e5: Large local-first uploads over the HTTP stream (a `createJazz` backend whose host can't upgrade sockets, such as `next start`) now complete. A session that received more than its 8 MiB ingress budget at once was dropped, and the upload started over on every reconnect, so a 100 MB file never arrived. The core now stops reading a session until its engine has taken earlier messages, and the backend passes that pace back to the client: an HTTP stream send is answered once the core has room for it, a socket is paused meanwhile (custom hosts handing sockets to `acceptSocket` can implement the optional `pause`/`resume`), and each HTTP stream send carries at most 4 MB. A client that keeps sending far past what the server takes, over a transport that can't be paused, still has its session ended.
- c5b21fb: `createJazz` names users `(jazz:backend, userId)` instead of `(jazz:app:<appId>, userId)`: a core admits only its own app's backend, so the issuer needs no app id. Pages that tell backend-signed-in accounts from local-first guests compare `account.identity.issuer` with `"jazz:backend"`. The alpha.59 import links returning users' old accounts to `(jazz:backend, sub)`, and keeps the alpha.59 app id in the new store's `app-id` file, so `sqlite({ path })` runs as that app, and its guests keep their accounts, without the app naming the id. A bucket or Blob store's default prefix is now `jazz/<store id>/`.
- 70f3002: Support `.includeDeleted()` in subscriptions for regular and history tables, with both local-first and remote reads. Subscriptions retain deleted rows' content, receive restoration updates, and fetch deleted content from upstream when needed. Server read permissions still apply.

  **Breaking wire change:** subscription registrations now carry `ReadViewSpec.include_deleted`. Include-deleted subscriptions have separate coverage keys; existing coverage keys and stored row/version formats are unchanged.

  Include-deleted subscriptions of regular tables accept the current source and snapshots (which transaction reads use), and reject branch views with a "not supported yet" error. Plain one-shot include-deleted reads still accept only the current source. History queries are unaffected.

  **Breaking Rust API change:** `ReadOpts.include_deleted` is removed. Rust callers now set the flag on the read view: `ReadOpts { read_view: ReadViewSpec { include_deleted: true, ..Default::default() }, ..Default::default() }`.

- a2249c0: A browser app whose IndexedDB runs out of storage space no longer loses writes or corrupts its local store. Before, every write still reported saved past the quota; then rows went missing, or the store reopened as "database disk image is malformed". Now the local store always holds a whole committed state: writes IndexedDB could not take stay in memory, keep syncing, and are saved once a retry succeeds. While saving fails, a write's `wait({ tier: "local" })` in a tab rejects with an error with code `storage` that names the cause, and opening a store with no room at all fails with that code too. A tab's local wait now resolves only once the write is in IndexedDB, about 1–2 ms later than before.
- 80794a1: Reads of rows that inherit read access from a parent (`allowedTo.read("...")`) now also bring along the rows the parent's own read policy checks, such as a `policy.shares.exists` membership. A freshly started backend, or a client that had never read the parent's table, used to return no rows for such a read until it had read those tables directly.
- 7e1ec5d: Destructuring the removed `isCreator` in `definePermissions` now fails with an error that names its replacement. An unsupported permission condition now names the rule it was passed to (such as `policy.todos.allowRead.where`) and what it got.
- b98f2d3: Local-first tokens are now minted and verified in TypeScript by a self-contained local-first auth module inside jazz-tools instead of by the browser and Node native runtimes (React Native still mints natively, with identical output). They are byte-identical to the tokens minted before, so existing local-first identities are unaffected. `ttlSeconds` for local-first tokens and identity proofs must now be a positive integer; 0 or a fraction throws instead of being coerced. `jazz-tools/passphrase` and `jazz-tools/passkey-backup` now come from that module.
- fad4bfe: JSON columns are now checked against their declared schema on every write path, in the engine itself: local inserts, updates and upserts, transaction writes, and writes the server applies for a client. Values large enough to be stored in chunks are checked whole, like any other. A value that does not match, or text that is not JSON, is refused with a `WriteRejected` error that names the column. Only values a write authors are checked: moving a row between branches carries its stored values as they are, even if the column's schema has been tightened since they were written.
- 793b3b1: A token signed with a key the cached JWKS lacks keeps failing (so the client retries) for as long as the identity provider is unreachable, instead of being signed out during the refresh cooldown. Only a JWKS refresh that succeeds refuses an unknown key. While that refresh cools down (up to 30 seconds), a token with a bad signature on a known key also gets a server error rather than a 401.
- a9d47ad: `fromJWT` documents how it caches a `jwksUrl`: for up to five minutes, refetched at most every 30 seconds for a token naming an unknown key. Publish a new key in your JWKS before your issuer signs with it.
- 4daf4a9: A large query answer no longer breaks the connection for good. An answer too large for one frame travels in parts, and the client read each part's marker byte as the start of a message, so a part whose next byte was 0 failed with "postcard u64 is not minimally encoded". The link then closed for good, and every later query on that database failed the same way until the process restarted (a large `offset` with `orderBy` on a 30,000-row table, on the backend's `jazz.db` and in browsers alike). The client now tells frames apart by their first byte, the way the Rust client does.
- 8f840ed: Queries no longer read the content of `string`, `bytes` and `json` values over 64 KB. Such a value now compares by identity: `eq`, `ne` and `in` never match it against a literal of 64 KB or less, a filter literal over 64 KB is rejected as not supported yet, and two columns, joins and `group by` treat two such values as equal only when they are the same stored value. `contains` and range filters (`gt`, `gte`, `lt`, `lte`) never match it, against a literal or another column, and in ascending order it sorts before every shorter value, in no meaningful order among other long values. Values of 64 KB or less filter and sort exactly as before. Reads still return the full value. In exchange, every query is plain SQL that never writes, and indexes on large-capable columns no longer carry a hidden sort-key column.
- 54763e3: Add pluggable chunk stores for large values: `DbConfig::with_chunk_store` moves the bytes of settled large-value leaves out of the SQLite file into a directory (`FileChunkStore`) or any asynchronous byte store, while their metadata keeps committing with the rows. Stores without a chunk store are unchanged.
- a771608: A table can declare `.history("latest")` to keep only each row's latest version, for rows overwritten often whose past is worthless, such as editor presence. Such a table can't be read through `history`, at a snapshot or in a serializable transaction, and can't use `branchBy`, text-merging columns or values large enough to be chunked. Switching an existing table to latest-only drops its history for good, so the first `createJazz` deployment that does it must pass `allowDroppingHistory: true`. Each write prunes by the schema its client runs, so rolling back to a full-history version stops the pruning; what was dropped stays gone.
- 51433d4: `createJazz` is synchronous and opens nothing when called, so a module can export `const jazz = createJazz({ ... })` and `export const { GET, POST, DELETE } = jazz.next()` at its top level, with no `await`, `globalThis` cache or wrapper. The store opens on first use: a request, a `jazz.db` call, `forReqUser`, `withAttribution`, `accountFor`, `linkIdentity`, or `await jazz.ready()` (which resolves to the Jazz, to fail at startup or to read `appId`). `jazz.appId` is known once the store has opened, and throws before.

  A process runs one Jazz per store (a `sqlite()` directory, a `vercel()` core): a later `createJazz` for it, as a dev server's hot reload or another module graph makes, returns the same Jazz and applies that call's app, permissions and options on its next use. Store settings that would need another core are refused while it is open; a credential hook is not such a setting, so the open store calls the latest call's. A reload that leaves the app and permissions as they were (equal ones rebuilt by a hot reload included) keeps the backend's session; one that publishes another bundle keeps the old session open a minute longer for the writes and `forReqUser` dbs still using it. Each `remote()` and `memory()` store stays its own.

  `jazz.db` works before the store opens: writes return at once (an insert's row, id included) and apply in order as it opens, reads wait for it, and subscriptions start then. A SQLite directory another process holds still refuses to open, and now says that is what happened.

  `jazz.db` holds at most 10,000 writes before the store opens, and a socket held meanwhile closes (1013) past 4 MiB. A failed open is tried again after a backoff (1s, doubling to 30s) rather than on every request, or at once by a later `createJazz`. `jazz.close()` applies the writes made before it, even while the store is still opening, and waits at most 30s for that open; from the close on, `jazz.db` throws. A `createJazz` for the store while it closes opens once the close has let go of it.

- 2ab8220: Migrations written before schema hashes moved to SHA-256 no longer apply. `jazz-tools validate` now moves them to `migrations/legacy/` and renames old-hash snapshots to their current hash, and the app runs on `schema.ts` as it is. createJazz publishes the generated `migrations/index.ts`, which never includes them. `jazz-tools validate --check` moves nothing and fails while any remain, for CI.
- cb4027c: Make staging many rows in one transaction linear instead of quadratic: a 10k-row in-memory batch write is about 4× faster.
- 91c9644: When the backend refuses to link a guest because the signed-in user already has an account (most often from another device), `linkWithBackend()` now says so and tells the app to call `loginWithBackend()` instead. The error keeps its `link_refused` code, and the guest stays signed in.
- 2dd0b70: After the background upgrade to the local-first client, local-first reads show the rows the app wrote while it was still on the thin client. Before, `db.all(table, { tier: "local-first" })` could miss the app's own write for a few seconds, even after the write's global wait resolved. Slow-query reports only suggest an index when the slow run scanned or sorted at least 1,000 rows.
- 89cdf50: **Breaking:** reads now have exactly two tiers, `ReadTier.LocalFirst` (`"local-first"`) and `ReadTier.Remote` (`"remote"`). A local-first read can wait a bounded time for the server on its initial load with the new `firstLoadRemoteWaitMs` option (`0` shows local data at once; see the client-default-first-load-wait changeset for the browser and React Native default):

  ```ts
  db.subscribe(app.todos, onRows, {
    tier: ReadTier.LocalFirst,
    firstLoadRemoteWaitMs: 2_000,
  });
  ```

  While the client is online, a subscription's first callback or a one-shot read waits up to that long for the server's answer and shows it; if the server has not answered in time, it shows local data. It never waits offline, without a server, after `db.disconnect()`, or once the connection drops. Later changes behave as for any local-first read. Rust clients get the same through `JazzClient::query_local_first` and `JazzClient::subscribe_local_first`.

  Removed read tiers now throw with a migration message:
  - `ReadTier.LocalFirstUnlessEmpty` (`"local-first-unless-empty"`, Rust `ReadTier::LocalFirstUnlessEmpty`): use `ReadTier.LocalFirst` with `firstLoadRemoteWaitMs`. Unlike the old tier, the wait also applies when the cache already has rows, but never lasts longer than the timeout.
  - The read names `"local"` and `"global"`: use `"local-first"` and `"remote"`. A one-shot `"remote"` read still sees your earlier writes, but a `"remote"` subscription shows your pending writes only once the server confirms them, where a `"global"` subscription showed them at once.

  Outside the browser, a client with a server now reads `"remote"` by default, where it used to read `"global"`. A one-shot remote read is answered after your earlier writes are uploaded, so it still sees them. This doesn't yet hold across a reconnect (#3863). This also fixes rows that briefly disappeared from a subscription when the server acknowledged the writes that created them, which only happened with the old `"global"` read.

  TypeScript rejects `firstLoadRemoteWaitMs` on a `"remote"` read; `tier` selects which options a read accepts.

  Write waits now accept only `"local"` and `"global"`. The deprecated `"edge"` overload is removed from TypeScript, and `wait({ tier: "edge" })` rejects at runtime with a `TypeError` instead of warning and waiting for `"global"`. Use `wait({ tier: "global" })` for server confirmation. The error rejects only the wait: the write was already applied, so wait on the existing handle with a valid tier rather than retrying the write. The durability semantics of `"local"` and `"global"` are unchanged.

- 1c3604c: Add `jazz-tools migrations graph <appId>` to visualize the migration graph.
- e1f8aff: When two migration paths between the same schema versions give different results, the refusal now names the table and column and how the paths differ (for example `for `todos.priority`, one path adds it with default 0 and the other adds it with default 1`), with the versions as `migrations/snapshots` names them, instead of two chains of full hashes. `jazz-tools validate` now refuses such paths, two different migrations between the same versions, and a snapshot whose contents don't match its hash, as a deploy does.
- 3601848: Allow migrations to rename tables that have relationships, including self-references and simultaneous reference-column renames. Preserve relationship targets through explicit table renames while continuing to reject changes to a different referenced table.
- 503a2d9: `jazz-tools migrations create` now refuses a schema change that keeps a column's name but changes its optionality or type, with a "not supported yet" error naming the column. Before, it wrote a migration stub that could not typecheck. A JSON column's schema and an enum column's variants count as part of its type: changing either on a kept column is refused too, since no migration step can change a column in place. To keep the values, add the new column under another name, copy the values into it from your backend, and drop the old column in a later release.
- 9f5b7ab: `jazz-tools migrations create` no longer writes add and drop steps for a column that may have been renamed. When an added column has the same type as a removed one, the stub leaves both without a step and a TODO that names the choice: `m.renameFrom("old")` keeps the values; `m.add` and `m.drop` start the column empty. `jazz-tools validate` refuses the file until the TODO's column has a step. Before, renaming an optional column silently made every value read null after deploy.
- 52a7995: Migrations get `m.add.uuid({ default })` and `m.drop.uuid({ backwardsDefault })` for a plain `s.uuid()` column. `jazz-tools migrations create` now writes them for such a column, instead of `m.ref("TODO_TABLE", ...)`, which named no real table and still validated and deployed. A column that references a table keeps `m.add.ref("table", ...)`.
- 3ca94c7: A column a migration adds with a default now gets that default on a local-first device that heard about the new version from the server while an older tab was still open. Before, the device's existing rows read `null` for that column even after reloading onto the new version, while a fresh device read the default. The sync server now sends each added column's default with the catalogue. A device that already added such a column as `null` keeps it that way until its local store is cleared.
- 9977b52: Fix authorised reads when one account claim is compared with both nullable ownership and required membership UUID columns. Finalise parameter types before checking predicates so anchored exact-base aliases consistently require the strongest UUID carrier in every comparison order and after canonical revalidation. Nullable-only groups retain nullable bindings, contradictory null checks reject consistently, and membership, containment and checked literal inference retain their operator-specific semantics.
- b21d332: A Node client no longer hangs when its upstream connection is established while a local read is suspended between evaluation turns. Synchronous binding calls now advance suspended reads instead of spinning on the state those reads hold.
- 7e56c40: React Native: when the server refuses a session's bearer (for example one that expired while the app was in the background), the native socket no longer retries it forever. It stops and reports the refusal, which reaches the app as an auth failure. The account then renews its token and reconnects, instead of the app crashing on an uncaught tick error. Other connection refusals (TLS, a proxy refusing the upgrade) are still retried.

  An account now renews on any refused credential, including one the server calls invalid, as after a signing key rotation. It does not renew when the app has that auth mode disabled, since a new token cannot fix that. A renewal that hands back an already-expired token renews again, under the usual backoff.

- f2a817e: React Native: a refused credential no longer fails the native foreground's ticks. The native socket stops and reports the refusal as connection state, the same way the web socket path does, so local reads and writes keep working while the app renews its token, and the renewed token reconnects.
- d54110a: React Native: a native upstream connection that fails for a reason other than the credential (TLS behind a captive portal, a proxy refusing the upgrade) no longer crashes release builds. The failure is reported as connection state, as on the web: the remote link shows as unavailable, local reads and writes keep working, and the link recovers when the connection does.
- 586d2df: New permissions reach every session at once. When a backend publishes a permissions bundle the core has not seen, the core ends every session pinned to another bundle with a retryable close (`NotReady`, retry later). Each client reconnects through whichever backend serves it now and is pinned again by that backend. This applies to every deployment of the app: a preview or canary deployment that publishes new permissions reconnects the sessions of all other deployments too, and they pin themselves to their own bundle again. Publishing a bundle the core already has ends no one: a cold start, a deployment that publishes the same permissions as another, or a rollback to an earlier bundle. Unpinned links (the system's, an admin's) are never ended. Schema-only changes still reach sessions through lenses, without a reconnect.

  There is no revocation list. To retire permissions that grant too much, deploy fixed ones and delete every deployment that still serves the old ones, including older production and preview deployments that are still reachable.

- 5ea793f: The core no longer keeps a current permissions head: every session is governed by the bundle it pins.
  - `GET /admin/permissions` and `fetchStoredPermissions()` are removed, along with the `StoredPermissionsResponse` and `StoredPermissionsHead` types. List bundles with `GET /admin/bundles` and `fetchPublishedBundles()`. `FetchStoredPermissionsOptions` is renamed `FetchPublishedBundlesOptions`.
  - Deploying admits a deployment's schemas (the first one included) without making any of them current, and a core no longer re-applies a stored head on startup. A server refuses a catalogue change that would make a schema current.
  - The `permissions_head_missing` rejection reason is removed: no write waits for a permissions head any more; a client meeting it from an older core treats it as an unknown rejection reason.
  - Rust: `prepare_catalogue_change` no longer takes a `revision`.
  - The migration graph (`GET /admin/migrations/graph`, `fetchMigrationGraph()`) reports `latestBundleSchemaHash`, the schema of the bundle published last, in place of `activeSchemaHash`. Bundles are stamped in publication order, so a bundle published in the same millisecond as another still sorts after it, and publishing an existing bundle again does not make it the latest. `jazz-tools migrations create` without a local snapshot uses that schema as its baseline, and the graph marks it "(latest bundle)". `fetchMigrationGraph()` still reads an older core's `activeSchemaHash`.

- 6d7aba2: The `jazz-tools server` and `jazz-tools create` commands are removed, and the package no longer ships native binaries. Jazz runs inside your backend: `createJazz({ store: sqlite({ path }) })` serves sync from your own server and `createJazz({ store: remote({ url, appId, secret }) })` uses Jazz Cloud. Start a project with `npm create jazz`. The CLI keeps `validate`, `schema compile`, `migrations create` and `migrations graph`, and the removed commands say what replaced them.
- abb6902: A device keeps writing when its account signs in another way. A local-first guest who signs up through the app's backend, or an account that later signs in under another issuer or a linked identity, keeps writing from the same device without "commit names a node another identity writes as" refusals. The node moves to the account's newest signed-in principal; a local-first guest key never takes a node back, so a leaked guest key can't write as a signed-up device.
- 676fed7: `jazz.nodeHandler` passes requests for the app's own routes on without touching their body, so mounting it after a body parser such as `express.json()` no longer turns every other POST into a 500 ("Response body object should not be disturbed or locked"). A request for a Jazz route whose body another handler already read now fails with an error that says to mount `jazz.nodeHandler` before body parsers; a buffer left by `express.raw()` is used as the body.
- e432352: Fix nullable group keys having different public representations in aggregate reads and subscriptions. One-shot reads now preserve NULL group keys and use the same logical result types as snapshots and live updates.

  Fix live aggregates losing nullable groups when their source rows change. Updates now find the indexed group using the same nullable key encoding, preserving counts, sums and group identity.

- a1ce928: An offline write that a tightened deployment refuses is now reported. Waiting for a write at the `local` tier no longer counts as handling a rejection that arrives after the wait resolved, so `onMutationError` fires when the authority later refuses it and the row reverts. A later write built on the refused one now fails with the refused write's cause, for example "the write was built on a write the authority rejected (permission denied: …)", instead of a bare "built on a write the authority rejected".
- 918c609: A `sqlite()` store is shared again between `next build` output and code Node runs unbundled (`next dev`, a `tsx` seed script, a custom server's own `createJazz`), for an app with migrations. Next's server build can load `jazz-tools` twice: once for the module that defines the app, once for the one that opens the store. The migrations `defineApp` records were private to the first copy, so the second opened the app without them, as though its current schema were its first. The store then refused one runtime or the other: "it was opened without the app's lenses into it" or "deployment cannot replace the runtime genesis schema". The app's schema now carries its migrations itself, so every copy reads them.
- 4d27968: Relation queries and relation-backed policies now share one relation IR and one lowering, so a relation means the same thing in both.

  Some permission policies that used to compile while silently ignoring part of their condition, and so granted more access than written, are now rejected when the schema is loaded. These shapes can be built with the `jazz-tools` permission builders:
  - Chaining `.where()` or `.join()` after `gather(...)` (or after a hop from it) so that the access rows are tied to the protected row a second time, for example `.where({ resource: todo.id, grant_role: todo.ownerId })`, or so that a joined relation itself filters on the protected row.
  - Filtering the rows a gather reaches while keying them on a column of the protected row, for example `.where({ id: todo.teamId, archived: false })`. That filter used to be checked against the protected row instead of the team. Put such filters in the gather's `start` or `step` instead.

  Policies built only from the supported single-correlation shape, such as `reachable.hopTo(...).where({ resource: row.id, grant_role: "viewer" })`, are unaffected.

  Hand-written relation IR changes:
  - Policies accept unbounded (`Fixpoint`) gathers.
  - Queries accept the scalar recursive step and seed hops that policies already accepted.
  - In queries, `= null` and `!= null` now test nullness, as the `jazz-tools` builders already emit them.
  - Both reject ordering comparisons against null.
  - A joined gather step may only project columns of its output row, named by scope.
  - A gather seed hop must name the hop target's `id` by scope.
  - Policies now reject, instead of silently dropping:
    - a row correlation (`= RowId(Outer)`) under `Or`, `Not` or an enum payload;
    - gather seed or edge filters that correlate with the protected row, and frontier row reads outside a gather step;
    - access rows correlated with the protected row more than once, correlated below their join, or joined to further tables;
    - a relation join on more than one equality, including inside a legacy `Exists` condition;
    - a correlation more than one join below the correlated root.

- 7cc229d: Text merge columns can now be optional: `s.string().optional().merge("text")` (and `"markdown"` or `"html"`) no longer throws. Concurrent additions to a `null` column are all kept, the earlier write first, as over an empty string. A write that sets the column to `null`, or a write that lands on a `null` someone else set meanwhile, is not merged: the later write wins the whole value.
- b2f42a9: Release private prepared query graphs when retained server subscriptions close. Clean up cancelled or rejected live binding admissions, including projected queries, without leaving orphaned bindings or disturbing other subscribers.
- 3efb401: Offline writes waiting to upload are now stored in tables shaped like the rows they write, with one row per written cell holding a plain SQLite value, instead of as encoded sync messages. A future change to the sync protocol no longer strands writes a user made offline before upgrading.
- a7472af: `createJazz` no longer makes its permissions current on the core when it boots. It publishes them as a bundle, addressed by its content, and the core governs every session that backend opens or relays (browser sockets, guests, and its own `forReqUser` writes) by that bundle alone. Two deployments of an app on one core now each serve their sessions by their own permissions, so a newer instance booting no longer takes over an older one's sessions, and a rollback is publishing the older bundle again. Upgrade the sync server before the app: an older server publishes no bundle, and `createJazz` refuses to start against it.
- 4e81ee8: Each live result reads as of the position its own read is settled through, and a one-shot read as of the connection's, whenever the local store holds newer versions from others. Against a sharded upstream a result then never shows part of a commit its read has not settled. Your own writes always show. Nothing changes against an unsharded server.
- 0f93fc8: Validate a createJazz session's serializable reads by its permissions bundle, so serializable transactions no longer always fail with a serialization conflict when the store's own permissions differ from the bundle's.
- df13889: Reject hand-written relation policies whose `Gather` the permission engine would have lowered to broader access than the relation describes: a Gather on the right side of a join, column frontier or dedupe keys, recursive or seed hops that are not an inner join onto the target row id, a step that projects anything but the target row id, and a seed keyed by a claim comparison other than equality. Policies built with the `jazz-tools` permission builder never produce these shapes. A stored schema containing one now fails to compile when it is published or loaded, with an "is not supported in policies yet" error, instead of granting the wider access.
- 702264f: Uploading a large value no longer blocks the JavaScript thread for seconds. Encoding each outgoing 1 MB piece took about 90 ms, so a 100 MB upload froze a Node backend's event loop (or a browser tab) for around 10 seconds. Large byte strings are now copied whole, which takes a few milliseconds.
- e6bdb4d: A write made through a schema version the server has not deployed yet is no longer lost in silence. Offline it stays pending and uploads under its version once the server has it; if the server still lacks that version when the write syncs, the server refuses it with the new reason `schema_version_not_deployed` (code `write_rejected`), which points to an app released ahead of its schema. A store that wrote before it became a replica now keeps those rows and refuses its upstream with an explicit error, rather than losing them on its first connect.
- 519e1e7: A write built on another write that the server rejected is now rejected too, with the new reason `predecessor_rejected` (code `write_rejected`). Before, it was applied over the server's row and brought back the values of the rejected write. This includes a delete made after a rejected edit of the same row: retry it once the row shows the server's version.

  App columns can no longer be named `rid`, `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched` or `_partition`: those names are taken by the store's own row columns.

- ed15adf: Add `jazz-tools/prosemirror`: keep a ProseMirror editor in step with a markdown text column. `toMarkdown` writes a document as canonical markdown with a map between editor positions and markdown offsets, and `markdownSync` with `applyMarkdown` writes local changes as whole values and applies merged values as the smallest change, keeping the cursor on its text.
- 9d815b3: `jazz-tools/prosemirror` adds overlays and selection presence: comments, highlights and collaborators' carets and selections are stored as text anchors and ranges and drawn as decorations, never written into the markdown. Each one follows its text through merged values, and comes back orphaned when its text is gone or drifted when it should be stored again.
- c510fd6: Expose `db.captureSnapshot()` and typed, versioned snapshot references for frozen branch bases. Add persistence helpers, explicit unavailable-history checks, native foreground capture support, and a maintained example covering restoration and draft edits.

  Existing compatible React Native builds continue to support existing operations. Only snapshot capture requires an updated native build; older builds report `UnsupportedSnapshotError` when capture is called.

  Open frozen subscriptions retain their captured base values through later writes and transaction fate changes. Reopening still requires the captured history to remain available.

  The TypeScript `snapshot` option now requires a `SnapshotRef` string. Replace hand-built snapshot field objects with `db.captureSnapshot()` or restore a saved string with `parseSnapshotRef()`; field objects passed to this option now throw `SnapshotRefFormatError`.

- 8d1692f: Query hooks now return the `db` handle next to their data, so a component that reads and writes needs one hook instead of two: `const { data: todos = [], db } = useAll(app.todos.where({ done: false }))`. This covers `useAll` and `useOne` in React, React Native, Expo, Vue and Solid, and the `QuerySubscription` / `QuerySubscriptionOne` classes in Svelte (`todos.db`). The React suspense hooks change shape to match: `useAllSuspense` and `useOneSuspense` return `{ data, db }` instead of the bare rows, so `const todos = useAllSuspense(query)` becomes `const { data: todos, db } = useAllSuspense(query)`. Vue's `useAllSuspense` / `useOneSuspense` gain `db` too. `useDb()` and `getDb()` stay for components that only write.
- 5a12685: Clearer errors for three easy React mistakes: `<JazzProvider serverUrl=…>` given a `fallback` prop now says to use `loading` (before, sign-in failed with "could not be cloned"); `useJazzSession()` under `<JazzProvider>` now points to `useJazzAuth()`; and a destructured `const { upgradeToLocalFirst } = db` now throws instead of resolving without upgrading.
- cb10f67: A client that reconnects after its connection to the server drops no longer fails with a `channel credit sequence mismatch`. Server messages the dropped connection had received but not yet processed are now discarded instead of being fed into the new connection.
- 1b67ead: A reconnecting client no longer sends frames produced for the dropped connection on the new one, which could fail the reconnected link with a channel credit sequence mismatch.
- 73afcef: A server now tells a client from 2.0.0-alpha.59 or earlier to upgrade, with an `unsupported_protocol_version` error that client can read. Before, the old client failed with "websocket received semantic frame before server hello", because the server's error frame used a newer layout.
- ee9ebd0: `createJazzClient` now refuses an option that holds a function or class, such as a `WebSocket` class, as soon as it is called. Before, the option reached the client's config and broke later, in the first sign-in, with "could not be cloned". No client option takes one; the client uses the platform's own WebSocket.
- adb7c8b: Reads over many rows whose relation-backed read policy refers to the row only by its id (an access edge naming the row, possibly reached through a recursive group closure) now evaluate that policy once per read instead of once per row. A first sync over many rows inheriting such a policy through their parent reads several times faster on the server. Write checks and rechecks of a few rows keep the per-row form, which seeks only that row's grants.
- 8a4345e: A tab keeps receiving updates to its own row through its worker after another user writes a version of that row on another branch that the tab may not read.
- 1b87158: A relay (a browser worker or a native relay) now refuses a session of another identity with the error reason `identity_mismatch`. The browser worker admits each tab as the author the tab connected with, and refuses one whose author is not its own. Before, it relayed such a tab's writes under its own identity, so the server attributed them to the worker's user. Tabs signed in as the worker's own user are unaffected.
- 7944b3f: Fix device relays that tick only when frames arrive (such as the React Native native relay) holding back what the server sent: a foreground's subscription now opens, and live changes arrive, in the relay tick that receives them instead of waiting for an unrelated later tick.
- 5fbb377: A browser tab served through a SharedWorker reads each live result as of its own upstream read's settle, the way a direct client does, and hears each move of the upstream's position even when no rows changed.
- 600c9fc: A `remote()` backend now reconnects after its core restarts. While the core was down, a reconnect attempt failed with only a socket error and never a close, so the backend waited on it forever: requests that needed the core hung, and `close()` had to wait out its sync timeout. Failed attempts now end, and the backend keeps retrying until the core is back.
- a7b8e1f: Fix remote reads and subscriptions asked right after a burst of writes sometimes missing those writes (most visibly in nested relations): the read now always reaches the server after the client's earlier commits. While the server's large-value upload limit holds a client's write back, that client's reads of the tables it writes (or whose permissions depend on them) wait for the upload to be admitted; its other reads are still served right away.
- 9eed461: A remote-tier subscription no longer stops for good when the sync server is unreachable for longer than 7.5 seconds. The client keeps reconnecting, and the subscription picks up changes again once the server is back.
- 9b077ff: Remove `jazz-tools deploy`. createJazz publishes your schema, its permissions and its migrations when your backend starts, so deploying the backend is the deployment. Running `jazz-tools deploy` now exits with an error that says so. `jazz-tools migrations create` and `migrations graph` are unchanged.
- d396059: Remove the `policy.<table>.managedByCreator()` and `isCreator` permission shorthands. Write the creator condition explicitly as `{ $createdBy: session.user }` on each rule instead, for example `policy.todos.allowRead.where({ $createdBy: session.user })`.
- 9515f60: **Breaking:** Removed `defaultDurabilityTier`. Reads default to `"local-first"` in browsers and React Native, and without a configured server; other environments with a server default to `"remote"`. Set `tier` on individual reads to override this behavior.

  Also removed, as part of the same cleanup:
  - The `tier` option on Node clients (`createJazzClient`). A backend that set `tier: "local"` with a `serverUrl` used to read local-first; it now reads `"remote"` by default and waits on the server. Pass `tier: "local-first"` on reads that should not wait.
  - The `clientId`, `runtimeSources` and `tier` fields of the TypeScript `AppContext` config. The client `runtimeSources` option is unchanged.
  - The `sqliteStorage` option on React Native `createDb()` (`ReactNativeDbConfig`). It was always rejected; storage is owned by the native runtime.
  - The React Native exports `UnimplementedSqliteStorageDriver`, `ReactNativeSqliteConnection`, `ReactNativeSqliteStorageDriver`, `ReactNativeSqliteTransaction`, `REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR` and `REACT_NATIVE_SQLITE_STORAGE_UNIMPLEMENTED_ERROR`.
  - The `--jazz-bin` flag of `jazz-tools validate`. It had no effect.

  TypeScript reports the removed options. Plain JavaScript ignores them silently, so remove them from your config.

- 7dcde2b: Breaking: remove the dev plugins. `jazz-tools/dev/next` (`withJazz`), `jazz-tools/dev/vite` (`jazzPlugin`), `jazz-tools/dev/sveltekit` and `jazz-tools/dev/expo` are gone, along with the managed dev server, schema watcher and env-file loading they ran. Jazz runs inside your own backend through `createJazz`, so there is no separate dev server to start. What's left is plain bundler config:
  - Next.js: `serverExternalPackages: ["jazz-napi"]`.
  - Vite, for the local-first client: `optimizeDeps: { exclude: ["jazz-wasm"] }`, with `jazz-wasm` as a direct dependency of your app (the plugin used to alias it for pnpm's strict layout).

  `jazz-tools/dev` and `jazz-tools/testing` keep `startLocalJazzServer` and `deploy()` for tests and fixtures.

- 4109163: Fix writes to a table or column a migration added being rejected with "value does not match type U64" once a SQLite core reopens (a dev-server restart, or a Vercel Sandbox core waking after its idle stop). A reopened core now knows every stored column by the type its schema version declared, not only the columns of the schema it reopens under.
- ad9ede6: The background upgrade from the thin client to the local-first client now happens in place. The session keeps its client and `Db`, and that `Db` hands its live queries, listeners and writes to the local-first one. Apps that kept the `Db` in a variable keep working. Framework providers no longer remount their tree (Vue, Svelte, Solid), so component state and half-typed input survive. React queries don't drop back to loading either.

  Signing in, signing out and switching accounts still give the session a new `Db`. The old one now ends its live queries with a `DbReplacedError`, and throws it on writes, `subscribe` and `onMutationError`, instead of going silent. The error says to read the current `Db` from `session.getSnapshot().client` inside `session.subscribe(...)`.

- c0f7b49: Stop a durable replica from reusing transaction ids after its file reopens without commits it already uploaded (a browser tab or worker killed before IndexedDB persisted them, power loss, or a restored backup). The authority deduplicated the new commit as a resend and the write was silently lost; file-backed replicas now number their commits no lower than their wall clock.
- a7809ed: `JazzBackend.forCurrentReqUser()` (`jazz-tools/effect/backend`) no longer takes the request body from the handler it wraps. It handed `authenticate` the whole request, body stream included, and on some effect versions (4.0.0-rc.118) that consumed the body, so a handler's `HttpServerRequest.schemaBodyJson` then failed. `authenticate` now gets the request's method, URL and headers, which is all it needs to decide who is signed in; the body stays for the handler.
- ef16565: Reserve the column name `_partition` for the partition sharded deployments store each row under: a schema can no longer declare an app column named `_partition`.
- 971bf7f: A reopened app reads the rows it kept as of the last position its server settled, and keeps naming its own writes accepted past that position, until the server settles again.
- 6ec4760: Restoring a row a client has not loaded now fails with the same `not_observed` error as updating one ("... is not loaded locally; read or subscribe to the row before updating it"), instead of claiming the row does not exist.
- 94b0641: Restore five guarantees from before the SQLite port:
  - A transaction's write policies are checked against everything the transaction writes.
  - Permission advice answers can no longer resolve a different request.
  - The Rust client reports a fatal sync error instead of stalling.
  - A later schema version can add a `branchBy` column that has a default.
  - A store keeps a refused transaction's payload until its host has reported the refusal.

- 087fbdd: Clients now resume their subscriptions from where they last settled when they reconnect, instead of being sent every row again, including sessions pinned to a permissions bundle (every `createJazz` client) and sessions with claims. The server names each session's read context in its `Welcome`: a digest of the bundle it is pinned to and its claims. The client stamps each stored watermark with that digest, and the server resumes only when the reconnecting session has the same one. Under another bundle or other claims the read opens fresh, so rows the new permissions hide are evicted and rows they newly allow arrive. Claims that change with every token, such as `iat` or `exp`, make every reconnect open fresh. `Read` and `Welcome` change layout on the wire, so clients and servers from before and after this change can't decode each other's messages and must upgrade together.
- 2925d33: React Native: `write.wait()` no longer fails with "undefined is not a function". React Native's `AbortSignal` polyfill lacks `throwIfAborted()` and abort reasons, which write waits rely on; Jazz now adds them where they are missing.
- 14c8e93: Large values can live in an S3-compatible bucket: `sqlite({ blobs: s3() })`, `vercel({ blobs: s3() })`, or `jazz-tools server --blobs s3`. On Vercel, connect an AWS S3 bucket to the project through the Marketplace: the core reads `S3_BUCKET_NAME`, `AWS_REGION` and `AWS_ROLE_ARN`, and assumes that role with the function's own OIDC token, so you set no keys. Elsewhere it uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (or `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE`). `s3({ endpoint })` reaches R2 or MinIO. Every chunk is put with its SHA-256, so the bucket rejects a corrupted upload. Bucket I/O runs beside the core's sync loop, so a slow bucket never stalls sync. A core warns when its bucket is in a different region from its Vercel region. A store on `"sqlite"` may move to `s3()`. Any other move is refused as not supported yet.
- ad480be: The `s3()` blob store now works out its bucket's region. On Vercel, `AWS_REGION` is set to the function's own region unless the project sets it, so a bucket in another region used to fail every request. Now, when the bucket answers that it is in another region, the core signs for that region from then on and logs a warning suggesting you set the region explicitly.
- c754b08: Several people typing at the same spot of a text-merge column now keep every character. Once their keystrokes had merged into one word, the next keystroke merged that word whole and dropped one person's letters; the same happened for numbers and for a word someone typed a space into. Core can merge such concurrent edits differently than before; upgrade every Core of a deployment together.
- f5ef995: Fix every session being refused with "permissions bundle … is published, but this store never admitted its write schema" after a deploy that only adds an index or changes a column default. The core now builds the new index, and serves sessions pinned to either deployment's bundle, before and after a restart; each writes with its own schema's defaults.
- 5d53912: Fix numeric ordering and equality for scalar float literals in query comparisons, including negative values and signed zero.
- e11c8df: Store and sync every row version at one ordered position instead of a (node, seq) pair. Unsettled writes now order last-writer-wins by their commits' hybrid clocks, so a worker and its tabs agree on concurrent edits even without a server. An explicit write time is recorded as `$updatedAt` but no longer orders the write, and a peer's clock is counted no more than 500 ms ahead of this store's own and never shifts the `$createdAt`/`$updatedAt` this store stamps. A tab that reconnects to a different or wiped worker drops the old worker's unsettled writes. A worker without a server keeps its tabs' writes pending until a server is added, and a serializable transaction waits for that server instead of being validated by the worker.
- 44d107b: Derive schema and inline permissions from one module evaluation. Without a separate permissions file, schema module side effects now run once per load, while independent loads remain isolated.
- c9d5de6: A backend's `forReqUser`/`forSession` read now returns rows the user can read through an `exists` policy on another table without that table having been read first: replicas receive the rows a correlated `exists` read policy rests on, as they already did for `allowedTo.read` parents. A reconnecting client no longer receives such rows again when it already holds them.
- a02d892: Reuse graph output descriptors within one permission proof to avoid repeated analysis of shared inputs, while preserving validation errors and keeping reuse scoped to immutable graphs and database state.
- 9267930: `reachable_via(...).seeded_by(...)` now takes a session reference, such as `session.user` or `session.claims["sub"]`, instead of a raw claim name. A string such as `"sub"` used to build a policy that the schema load rejected; it now throws when the policy is defined.
- 73ff2ce: Fix the server spending CPU exponential in the policy depth to sync a table whose read policy consults its own table, such as "members of a workspace read its members". In BandBook, the fourth member to join a workspace took about a minute of server CPU; it now takes milliseconds.
- 4c48833: BREAKING CHANGE: Separate schema and migration namespaces; remove the combined `col` export.

  ```ts
  import { schema as s, migration as m } from "jazz-tools";
  ```

  - Replace `col` column builders with `s` (e.g. `col.int()` → `s.int()`).
  - Move `add`, `drop`, and `renameFrom` calls from `s` or `col` to `m`.
  - Replace `s.defineMigration` and `s.renameTableFrom` with `m.defineMigration` and `m.renameTableFrom`.

  Schema and permission helpers stay under `s`. Migration behavior and serialized formats are unchanged.

  Rewrite existing files in `migrations/` too, including already-deployed ones: `jazz-tools validate`, `jazz-tools migrations graph` and `deploy()` load the whole migration chain, and a file that still uses the old builders now fails with `Failed to load migration <file>` and a pointer to `migration as m`.

- 2a7c8ab: Keep the Jazz server inside its memory limit under load. The server now caps live WebSocket connections (default 1024, `--max-connections` / `JAZZ_MAX_CONNECTIONS`) and answers further upgrades with `503` and `Retry-After`, closes connections that send nothing for 60 seconds, and sizes one shared storage cache from the container's memory limit (`--memory-limit-bytes` / `JAZZ_MEMORY_LIMIT_BYTES` to override) instead of giving each internal store its own 256 MiB cache.
- 59dd3a5: A Jazz server keeps each app's rows, schema catalogue and accounts in one SQLite file, `jazz.sqlite`, in its data directory.
- 90e0a42: A row one subscription stops listing now stays on the device while another live subscription still holds it, whether as a result row or as a row it pulls in through an include, a nested array, a join, a gather edge, a policy input, or a union arm. Previously, after a reconnect, a row could disappear from a subscription that still needed it, because the server would not send it again. The server now tells the client which rows each live read holds, using a new `Holds` sync message sent with each read's opening and whenever that changes. The client drops a row only when no live read holds it. This adds a message to the wire, so clients and servers must upgrade together.
- 3e97129: Breaking: `session.user` is now the account, and `session.identity` is the exact login (`{ issuer, subject }`). This holds in permissions and on the client session (`useSession()`, `useAuthState()`, `db.getAuthState().session`).
  - `{ $createdBy: session.user }` matches rows written through any identity linked to the account, including alpha.59 rows whose author was imported with an account. `{ "$createdBy.identity": session.identity }` matches only the exact login.
  - Replace `session.user.account` with `session.user`, and `session.user.identity` with `session.identity`. The old forms throw an error that names the replacement. A mismatched comparison, such as `$createdBy` with `session.identity`, is refused when the schema compiles and names the forms that compare.
  - An accountless session (an anonymous reader) has `session.user === null`, so `{ $createdBy: session.user }` matches no row for it.
  - `{ $createdBy: session.user }` used to match only the exact login that wrote the row. It now matches the whole account; if you relied on the exact login, switch to `{ "$createdBy.identity": session.identity }`.
  - Redeploy your permissions together with the upgrade. A stored permissions bundle that still uses `session.user.account` or `session.user.identity` no longer compiles on the new server, so reads and writes under it fail until the new permissions are published.

- d9a4cbc: Settling many pending writes no longer slows down with the number of writes still pending. Before, every write the server settled replayed every other pending write, so a backend inserting 20,000 rows took minutes to settle them, and a read issued right after the inserts failed with "Timed out waiting for query coverage". A settled write now replays only the pending writes that share its rows, so settling 5,000 inserts takes about 2 s instead of 160 s, and the inserts themselves run several times faster. The speedup applies to writes on different rows: writes that all touch one shared row (a counter every write bumps, say) still replay together. A pending write through a branch view now also shows the base branch's new version as soon as it arrives.
- 51d24f9: Jazz now uses SHA-256 wherever it used BLAKE3. RFC 9562 UUIDv5 ids (account, app and object ids) are unchanged. Large-value chunk nodes are identified by plain SHA-256 over their encoding, so object stores such as S3 and R2 can verify direct client uploads against the node hash, and JavaScript can compute Jazz's content hashes and derived ids with WebCrypto. Schema hashes change too, so `jazz-tools validate` rewrites the hashes in your generated `migrations/index.ts`.
- b542178: Share a maintained subscription's output tables instead of copying them on every update, and borrow table schemas during query validation instead of cloning them on every lookup. This removes a large share of the schema copying a client does while it syncs a schema with many tables.
- 0af778a: Local-first auth hooks for React, Vue, Svelte and Solid now share one controller per secret store. Consumers of the same store share a single secret load, and a consumer that mounts later no longer reloads the secret. `login` and `signOut` reload every consumer even when saving or clearing the secret fails. A synchronous secret-store throw is reported as an auth error. Vue and Svelte warn once per distinct load failure.
- e32ee34: Servers now share one compiled query graph between subscribers of the same query. Previously every connected subscriber got a private copy, so server memory grew by a full query graph per open subscription. Each subscriber still sees only the rows its own permissions allow. Subscriptions whose query parameters differ, for example a chat id, still get separate graphs for now (#4017), and so do queries whose graph cannot yet keep subscribers apart, such as a count over rows with per-user permissions.
- 1d2632a: Reuse template expansion and binding isolation for shared inputs across the terminal outputs of one query, preserving permission checks and session isolation.
- 699ab44: Every client now reconnects to its server the same way: React Native and the Rust client back off with jitter (100 ms up to 5 s) and report an outage after 7.5 s, as the web and Node runtimes already did. The Rust client no longer retries a refused token; it waits for a new one, which `JazzClient::update_auth_token` provides.
- 625ca5e: Signing in works behind a host's JSON body parser. The browser asked the backend who is signed in with an empty POST typed `application/json`: Fastify refused it with 400 (`FST_ERR_CTP_EMPTY_JSON_BODY`), and `express.json()` mounted before `jazz.nodeHandler` swallowed it, so Jazz answered 500 and the browser stayed signed out. Requests without a body no longer carry a JSON content type. `nodeHandler` serves an empty body a parser already read, and a JSON body `express.json()` parsed (linking a guest sends one) as that JSON. A Jazz request whose body another parser took in another form still fails, now with the code `request_body_already_read`, which the client reports instead of a bare server error.
- 51cfde5: Signing out no longer fails once the server refuses this user's sign-in, and neither does being signed out by the backend. That covers a backend session that ended, an expired credential, and another user who signed in through the app. Before, a user whose session ended with a write still on the device was stuck on "Graceful shutdown could not synchronize pending writes; the context remains open", through Retry, sign-out and reloads, with no way back to sign-in. When another user signed in on a local-first browser without signing out first, every tab was stuck on that error instead of switching to them. Jazz now signs out (or switches) and warns about the refused writes. A persistent client keeps them on this device and uploads them when that account signs in again. A client that keeps nothing says it dropped them. An expired credential counts as refused: sign out after renewing it if its writes must reach the server first. A sign-out during an outage still fails, so the app can retry once the writes can upload. With `clearOnSignOut`, a sign-out now also warns how many writes that hadn't reached the server its wipe removed.
- 2a74594: `jazz.forReqUser(request)` and `jazz.withAttribution(request)` on a request that `authenticate` names no user for now throw a `RequestAuthenticationError` with `code: "signed_out"`, so `isRequestAuthenticationError(error)` maps it to a 401. They threw a plain `Error` before, which the documented mapping turned into a 500. An `authenticate` that throws still passes its own error through. In `jazz-tools/effect/backend`, the `JazzError`'s `cause` is that error, so a handler can tell a signed-out request from `authenticate` failing.
- 66fa2ac: A local-first guest who signs up in one tab while another tab is open no longer has the tab they signed up in stop syncing. That tab's new client was refused by the shared worker, which the other tab still held as the guest. It ended `offline` (`stopped`), read nothing, and its writes never uploaded. Now it shows `connecting` and joins once the other tab follows the sign-up, or goes away: the worker now notices a tab whose page closed, crashed or was discarded without closing its client, and lets go of what it held. Queries, waits and writes made in the meantime go on once it joins; writes stay in that tab's memory until then, so a reload before it joins still loses them.

  If the other tab doesn't let go within 30 seconds (it is frozen in the background, or runs a build that doesn't follow sign-ups), the tab reports `{ state: "offline", reason: "held-by-another-tab", retrying: true }` and fails what waits for the store with a `StoreHeldByAnotherTabError` (check with `isStoreHeldByAnotherTabError`), so the app can ask the user to close or reload the other tab. It keeps trying in the background and joins as soon as it can. Any other configuration a worker refuses, such as an older deploy's schema in another tab, still fails visibly.

- 8dfebc7: A web or Node client now notices a connection that went silently dead, such as a half-open TCP link or a stalled proxy, and reconnects. Before, it kept showing `connected` (or a thin client stayed `connecting`) while nothing arrived and pending writes never landed, until the page was reloaded. The server now sends these clients a small heartbeat with its keepalive ping every 20 seconds, and a client that hears nothing at all for two minutes drops the link and reconnects. Clients on the native transport (React Native, and backends using `remote()`) don't have this yet.
- b0ff14b: `linkWithBackend()` no longer sends the guest's sync token. The client asks the backend for a challenge that names the signed-in user and expires after two minutes, and signs it with its local-first key. The core links on each proof once. A leaked, unexpired guest token can no longer link the guest's account to someone else. The local-first auth module gains `signChallenge` and `verifyChallenge`.

  `createJazz({ guestKeyAfterLink: "revoke" })` retires the guest's local-first key once it links, so only the signed-in user opens the account from then on. The default, `"keep"`, leaves the key usable as today.

- 5ee8ead: Jazz now logs slow queries on every host: the server, browser tabs and workers, React Native, Node backends, and the Rust client. Each warning names the app and one query shape that held the database for 50 ms or more, with how often it ran, how long it took in all, how many rows it scanned, SQLite's query plan, and which index would likely make it fast. The first slow shape is reported at once, later ones at most every 10 seconds. JavaScript clients write it with `console.warn`; the server and the Rust client write it with `tracing`. Change the threshold with `slowQueryThresholdMs` in the client config (or `JazzClient::set_slow_query_threshold` in Rust, `JAZZ_SLOW_QUERY_MS` on the server); `0` turns the log off.
- d32a042: A sync server no longer holds unbounded output for a client that reads slowly or stops reading. Once what earlier ticks queued for a session and the client has not read passes twice the session's biggest single tick (at least 64 MiB), the server closes the session with `Backpressure` and asks the client to retry later, instead of growing until the process runs out of memory. One tick's output, such as a large first result, is always sent whole.
- c8d47ff: Store a pending upload a row per op instead of as one encoded message, so a
  store with a small value limit (a Durable Object's 2 MiB) can keep commits
  that travel in several sync frames. Refused commits kept for retry store
  their ops the same way.
- 68dd540: Jazz stores and queries data in SQLite on every platform: a SQLite file on Node, React Native and the server, and a SQLite file on IndexedDB in the browser. Local stores written by alpha.59 and earlier (native, native relay, Core and browser) are refused with `UnsupportedStorageCodecs` (`unsupported_storage_codecs`) and left unchanged. See the upgrading-from-alpha-59 changeset for what to do with them.
- ff66014: Large values' bytes can live outside the SQLite file. `sqlite({ path, blobs })` takes `files({ dir? })` (the default, one file per chunk in `<path>/blobs`) or `"sqlite"` (inside the database file, not recommended). The sync server takes `--blobs files|sqlite` (default `files`) and `--blobs-dir`. Rows and the chunk index stay in the database; bytes move out on later ticks, so an existing store moves its large values out by itself. A store keeps its blob store after that: switching back to `"sqlite"` or to another directory is refused as not supported yet. The `vercel()` store's Sandbox core keeps large values in SQLite until it gets a network blob store.
- 10e2c3b: Keep a pending transaction read at the writes staged when it began, even when later writes arrive before the authority replies. The final commit still includes all staged writes.
- b22d0a9: A transaction whose permission checks read many of the rows it writes (for example a long chain of rows that each reference the one before) no longer fails on a Durable Object SQLite store with "variable number must be between ?1 and ?100". The check now passes those rows as one parameter.
- b91cb28: Preserve optional storage-format identifiers when rebuilding storage refusal
  errors from bindings or worker messages. Keep the original error unchanged when
  its codec lists are incomplete or its format identifier is malformed.
- cf4080e: Give every table and column a stable identity derived from the authority's
  stored names, mirrored to replicas in the sync catalogue. A replica refuses
  an upstream that belongs to another app, and a column that is dropped and
  added back keeps its identity and values only if its type is unchanged.
- 793a601: Abort failed streaming uploads without waiting for application source cleanup.
  Release browser reader locks and request unfinished iterator cleanup while
  preserving the original upload or source error.
- d7d686c: A browser behind a proxy that never answers WebSocket upgrades now syncs. Examples are `vite preview` without `ws`, or a fetch-based proxy in a framework's server hooks. Before, the socket stayed connecting forever, so the HTTP stream fallback never started and nothing synced. Now a socket that hasn't opened after 10 s counts as failed. If the backend answers plain requests and says it upgrades sockets, two such hangs in a row switch the page to the HTTP stream, with a console warning that says why. It takes two because a scale-to-zero host can hold the first upgrade after idle while it starts.
- 137a57f: A browser on a backend that upgrades sync sockets (`attach()`) now falls back to the HTTP stream when it sits behind something that doesn't pass WebSocket upgrades on, such as `vite preview` with `ws: false`, a framework's fetch proxy, or some PaaS edges. Before, it retried the socket forever and writes never synced, with no error. After three sockets in a row fail while the backend itself answers, it switches to the stream for as long as the page stays open, and logs a warning that says why. A backend that is down or restarting in between starts the count again. If the backend refuses the page's origin (a proxy that rewrites the Host header), the client says so once and stays on the socket, because the stream would be refused too.
- 761cdba: Transactions now take `insertStreaming`, `updateStreaming` and `upsertStreaming`: the streamed value is staged in the transaction and commits or rolls back with it, in local-first, React Native and thin clients alike. A streamed value is held for its transaction for the store's staging max age (10 minutes by default); a transaction left open longer is refused at commit with `streaming upload expired`.
- 4bd3309: Keep streamed uploads bound to their original database, schema and target column. Reclaim abandoned uploads during normal maintenance while preserving live streams and committed values. Cleanup waits until an open transaction ends, so rollback cannot lose an abandoned upload’s reclamation request.
- c584159: Subscriptions can deliver a `string` or `bytes` column's changes as splices: name the columns in the `splices` query option and read them from `onUpdate(rows, { splices })`. On a range select the splices are relative to the selected window. Columns of other types, transformed columns, and range-selected columns that a computed field reads are refused as not supported yet. The React Native native relay ABI is now 4, so the JavaScript bundle and native build must match.
- e678a03: Fix `useAllSuspense` and `useOneSuspense` reporting a hook-order change (and a recoverable concurrent-render error) when their first result arrives before React yields.
- beb79c4: A sign-out or a switch to another user no longer warns about refused writes when the previous user left none behind.
- bec88ce: A serializable commit refused before it is sent is reported as serializable. A server that cannot send to a session now records why for its host before closing it. Pending uploads stored in another layout are refused as an unsupported storage format, and a settled upload's stored rows are removed in one statement.
- 803a188: Add a sync status API. `db.getSyncStatus()` returns the upstream connection (`local-only`, `connecting`, `connected`, or `offline` with a reason) and how many committed transactions the server has not accepted or rejected yet, plus a `synced` flag. `db.onSyncStatusChange(listener)` reports changes, and `db.waitForSync({ timeoutMs })` resolves once every earlier write reached the server, waiting through outages like a `global` write wait. In the browser, both count only the current tab's own writes.

  The frameworks get the same status: `useSyncStatus()` for React, React Native, Solid and Vue, `syncStatus()` for Svelte, and a `syncStatus` stream plus `waitForSync` in the Effect layer.

- 2691885: Fix a browser tab that could rarely stop hearing from its shared worker right after it connected, which left its queries and waits hanging. The worker watches a lock each tab holds for as long as its page lives, so it notices a tab that went away. It could ask for that lock before the tab held it, get it at once, and drop the live tab. A tab now names its lock to the worker only once it holds it.

  Also, a backend watch that keeps getting 404s now stops after exactly its third answer. It had compared the clock with its retry waits, and a 1 ms disagreement between them could add one more retry.

- c63b143: Fix a browser tab's write failing with a storage error ("UNIQUE constraint failed") after its worker shared another tab's unconfirmed change to the same row, and keep a tab's own unconfirmed inserts its own while its worker uploads them.
- d73ddff: Add `db.textAnchors()`: capture cursors, selections and comments on a text value as plain `{ before, after }` anchors and `{ before, matched, after }` ranges, and find them again in later values with a confidence and an ambiguity flag. Tools and LLMs can write them from the current value alone. `jazz-tools/prosemirror` adds `captureAnchor`, `resolveAnchor`, `captureRange` and `resolveRange` for editor positions. Browser, Node and React Native runtimes support them.
- 6013e92: Two people typing into one text-merge column at once no longer lose characters. `db.update` takes `editedFrom: { column: value }`, the value an editor last showed or wrote; when a merged value reached the local store before the editor showed it, the write is merged over that value instead of undoing it. `markdownSync`'s `onChange` now receives `{ editedFrom }` as its second argument; pass it on: `onChange: (body, { editedFrom }) => db.update(app.notes, id, { body }, { editedFrom: { body: editedFrom } })`. Merges also keep both sides' letters when two people type into the same word. `editedFrom` is not supported yet in branch views or on columns with a transform, and throws there. Because of the in-word merge, Core can merge the same concurrent edits differently than before (`cat` edited to `cart` and to `cast` now gives `carst`); upgrade every Core of a deployment together.
- 4b94368: New `textFieldSync` in `jazz-tools/text-field` keeps a plain `<textarea>` or `<input>` in step with a text merge column. It writes each change with `editedFrom`, and applies merged values as a change, so the cursor stays where it was and no keystroke is lost when several people type at once. The `<textarea>` snippet in the collaborative text editing recipe used to lose characters and move the cursor to the end; it now uses `textFieldSync`, with an uncontrolled field (`defaultValue`).
- a638a1c: Typing into a long text document now costs a few hundred bytes per keystroke on the wire and in history, not a copy of the document. A whole value written over a chunked one is stored as the splice that makes it, a text merge on the server is written the same way, and text columns with a `text`, `markdown` or `html` merge strategy are chunked from 4 KB instead of 64 KB. Filters and sorting do not read those columns' values over 4 KB, as for other values over 64 KB.
- ee4de47: Add opt-in text merging for string columns: `s.string().merge("text")`, `.merge("markdown")` or `.merge("html")`. The column still stores a plain string (large values included), and concurrent edits to it merge instead of the last write winning. Disjoint edits all survive, an offline chain of edits merges each one over what its writer saw, and where two writes replace the same span the later one wins. A write built on an edit the server rejected is rejected too. Markdown and HTML merges keep delimiters, link targets, tags and entities whole, but are not tree-aware yet. A schema naming a merge strategy this version does not know is now refused by name.
- 95e49a9: When a text merge runs past its work budget and the later write stands as sent, the server now logs a warning that names the app, the table, the row and the column, because a concurrent edit has left the current text (history keeps it). Until now this happened silently.
- 5b2ee4c: Text merge columns now keep an edit with its paragraph, list item or sentence when someone else moves that block at the same time, line up renumbered lists by their words, keep a trailing newline at the end, and never leave an unmatched tag in HTML columns.

  Known limitation: when two people move the same section to different places at once, the section ends up once but a stray `## .` is left where it was (#4646).

- d95da94: Browser apps now start on the thin client: `<JazzProvider>` (and the Vue, Svelte and Solid providers, through `createJazzClient`) opens a thin client unless told otherwise, so the first load fetches no WASM, worker or local storage. Pass `client={() => import("jazz-tools/local-first")}` to upgrade to the local-first client in the background once the page has loaded, or call `useDb().upgradeToLocalFirst(() => import("jazz-tools/local-first"))` to upgrade on demand. A browser that upgraded an account before opens it local-first straight away, so it works offline. `jazz-tools/react` (and the other framework entries) no longer import the local-first client statically; `createAccountManager` and a provider given an explicit `config` load it on first use. The browser account store behind `createAccountManager` from `jazz-tools/thin` now needs Web Locks, as the local-first one already did, and throws without them; outside a browser with Web Locks, pass an atomic `store`.
- d697c53: Add `jazz-tools/thin`, a thin client that asks the server for query results instead of syncing a local replica: no WASM, worker or local storage, the same `Db` API, and self-signed sign-in in plain JavaScript. Writes, transactions, branch and snapshot views and large-value streaming all run on the server. Offline writes are not supported in thin mode yet, and a transaction open when the connection drops (including the reconnect a credential renewal takes) fails with `ThinTransactionLostError` instead of committing.
- 6245efe: After the in-place upgrade to local-first (under a Jazz provider or `createJazzClient`), a read the app made on the thin client while the server was down no longer stays with the thin client, which waits for its own unanswered writes. The local-first client takes the read over. A `local-first` tier read answers from what that client has. A read at the remote tier still waits for the server, now through the local-first client.

  On the thin client, a write that may have reached the server before the client stopped for good now fails as having an unknown outcome, not as rejected. A write that never reached the server, whether the client closed or stopped for good, fails with `ThinRequestNotSentError`. A mutation-error listener reports it with code `not_sent`, not `outcome_unknown`.

  The standalone `upgradeToLocalFirst` from `jazz-tools/thin` now returns even while the thin client waits for the server to answer its writes. A read on the old thin `Db` that never reached the server fails with `ThinRequestNotSentError`, now exported from `jazz-tools/thin`, so it can be made again on the new `Db`. Shutting the new `Db` down also closes the old thin runtime.

- 0d2c69b: A thin client connected again with a different server URL or credential now ends any write that is waiting to be resent as `outcome_unknown`, as `updateAuth` already does, instead of resending it as someone else. An open connection bound to the old credential is also replaced: before, `connect()` kept it.
- fb5c8fa: A thin client whose server can't serve its schema (a bundle built against a schema that isn't deployed) now fails its reads and writes with the server's error instead of leaving them pending forever. A `db.all()` or `db.one()` that was running on the thin client when the session upgraded to local-first now gets its answer from the local-first client instead of rejecting with "the thin runtime is closed". Other one-shot calls cut off by the upgrade still reject.
- 6693496: Thin clients read and write rows holding values over 2 MiB (up to 32 MiB per request or result): such a request or result now travels in parts instead of overflowing one frame and closing the connection. A result over 32 MiB fails only its own query with a `TooLarge` error, and a write that large is refused before it is sent (use `insertStreaming` / `updateStreaming`); the connection and other queries carry on.
- 86c6718: The thin client (`jazz-tools/client` before its local-first upgrade) now reports its link in `getSyncStatus()`: connected, offline after a sustained outage, and offline with reason `"auth"` when the server refuses its sign-in. Before, it always said "connecting", so a browser whose user lost their account on the core (a reset dev store, a restore, a new core) never signed in again: its writes hung and "Account auth refresh failed" repeated, even after a reload. It now signs the user in again, as documented.
- e19eaab: A thin client reading a large value whose bytes live only in an `s3()` (or other network) blob store, for example after the server restarted, no longer hangs the server. Before, the read waited for a fetch the busy server could never finish, and every later request timed out until a restart. Now the request waits while the server fetches the bytes in the background, then answers; other clients are served meanwhile, even when the bucket is slow or down. If the bucket fails, the read answers with the storage error instead of waiting, and later reads fail fast until the server's retry backoff ends. Reads inside a transaction and large-value writes go the same way.
- e15fe09: Thin clients now report a core error's stable snake_case code (`transaction_conflict`, `storage`, ...) as `error.code`, the same code a local-first client reports. Before, a thin client got the Rust variant name (`TransactionConflict`), so code that retried on `transaction_conflict` missed a thin client's serializable conflict.
- a0b962f: Thin clients (the default) can write text edits again: `db.update(..., { editedFrom })` and `applyDiffs` splices on large values no longer fail with `NotObserved: large-value cell is not observed`. The server read the cell under its own schema instead of the session's published permissions, so every row looked unreadable.
- 159c666: A thin (server-first) client no longer loses a write when its connection drops before the server answers. Before, such a write ended as `outcome_unknown`: it may or may not have been saved, and the app couldn't tell. Now each write carries the client's own id for it, and the client sends unanswered writes again, in order, once it reconnects. The server recognises a write it already made, through any server of the same core, and answers with the first outcome instead of writing it twice. So a resent update never undoes someone else's later change. An explicit `disconnect()`, a credential change (`updateAuth`), large-value updates, and streams still end unanswered writes as `outcome_unknown`.
- a9bad76: Reject attempts to commit a serializable transaction through the mergeable
  commit API. A mismatched commit discards its staged writes instead of bypassing
  serializable conflict checks.
- 03b6333: Make LocalOnly transaction reads return from local data without waiting for the
  server. Keep the snapshot fixed after that read, including when a later read
  requests remote coverage.
- d1e4540: jazz-tools server: `--trusted-workload-issuer/-audience/-subject` makes a server trust one OIDC workload (for example a Vercel function) instead of shared secrets. Every request and WebSocket upgrade must carry its token in `X-Jazz-Backend-Credential`; the backend states its access (`backend` or `admin`) in `X-Jazz-Backend-Access`, and secrets a request or prelude carries itself are ignored.
- 1fc7344: Programmatic `deploy()` (from `jazz-tools/dev` and `jazz-tools/testing`) accepts historical `schemas` and multiple `migrations`, so apps can publish concurrent migrations from separate branches. It publishes the schemas and migrations passed to it that the server lacks, and its result lists the `changed` and `published` schemas and migrations instead of a single migration and permissions.
- 113b65f: Preserve each UNION arm's ordering, offset and limit before applying the outer
  UNION page. Automatic arm labels distinguish windowed seeds with a versioned
  canonical preimage while retaining labels for unmodified seeds.
- d9750c2: A JSON column's `uniqueItems` check now treats 0 and -0.0 as equal in arrays of any length, natively and in the browser. Before, native arrays of more than 15 items were checked with a randomly seeded hash, so the verdict on such an array could change from run to run.
- cb7cbc3: Dropping a Rust client subscription stream now unsubscribes right away, ahead of whatever the client sends next on the same connection, instead of on the next tick. Before, a write landing in between could still reach the dropped subscription's local store.

  `SubscriptionStream` is no longer `Send`: it now holds a handle back to its client, which is single-threaded already, so receive it on the client's thread.

- cab488a: A browser that opens with the server unreachable (a first visit offline, an outage) now still upgrades from the thin client to local-first. Before, the upgrade read the app's live queries from the server first, failed, and left the app on the thin client for the rest of the page's life: a guest's writes stayed in memory, didn't show in its queries, and were lost on reload. The upgrade now waits for those reads only while the server answers, and doesn't wait for the thin client's unanswered writes either: they upload in the background once the server is back. Writes made after the upgrade go to the local store. Writes made on the thin client before it still live only in memory until the server answers them, and a reload before then loses them.
- 144326e: ### Upgrading from alpha.59

  alpha.60 is a breaking release for every app. Upgrade in this order:
  1. **Sync server first.** An alpha.60 sync server (core) speaks wire protocol v6 and admits only its app's backend, so alpha.59 clients can't reach it, and `createJazz` refuses to start against an older core. Plan one cutover per app, and ship the new backend and clients with it, because both change. Clients can't sync from the moment the alpha.59 server stops until the new backend and client bundle are live, so plan for that downtime, and let clients finish syncing first: writes still queued in alpha.59 browsers are not carried over. On Jazz Cloud you start the upgrade yourself, when your new backend and clients are ready to ship; we don't move your app for you. On your own server, stop it, move its data dir with the alpha59-alpha60 importer, and drop the JWT flags and their `JAZZ_*` variables (see core-admits-only-its-backend). Download the `jazz-alpha59-alpha60-export` and `jazz-alpha59-alpha60-import` binaries for your platform from this release's GitHub assets, export the old data dir with the exporter, then run `jazz-alpha59-alpha60-import import` and `jazz-alpha59-alpha60-import verify` before your backend first starts on the new dir (see alpha59-migration and the Backups & Upgrades docs page). The import checks the result against what alpha.59 reads, and links each user's old identity to `(jazz:backend, sub)`, so returning users keep their accounts when your `authenticate` answers with their old `sub`. Later releases keep the importer, so you can upgrade from alpha.59 whenever you're ready.
  2. **Your backend.** Add `createJazz` with `remote({ url, appId, secret })` (or `sqlite({ path })` on the imported data dir) and an `authenticate` that reads your app's own session (an app that signed users in with a JWT can use `fromJWT()`, which answers with the token's `sub`), and mount it at `/jazz` (see client-signs-in-through-backend). If your app has local-first guests, return `acceptLocalFirstAuth(request)` from `authenticate` for them, or the backend refuses them (see local-first-guests-opt-in). Other machines that used `createJazzSession` call `createJazzClient` now. Your backend now publishes the schema, permissions and migrations when it starts. `jazz-tools deploy` and the dev plugins are gone, and so are the starters: `npm create jazz` now prints a brief for your coding agent instead of scaffolding an app.
  3. **Your schema.** Import `schema as s` and `migration as m` instead of `col`, add `import { migrations } from "./migrations"` and pass them to `s.defineApp(schema, { migrations })`, then run `jazz-tools validate`, which writes `migrations/index.ts`. `defineApp` throws without migrations, so add the import before running the CLI.
  4. **Your clients.** Drop `appId` from browser clients (pass `scope` with the old value to keep existing local-first identities). Point `serverUrl` at your backend (`"/jazz"` by default) and remove JWT auth. Browser and React Native apps, the Node and WASM bindings and the React Native native build all move together: the native relay ABI is now 4, so a JavaScript bundle and a native build from different releases refuse each other.

  Local data on devices doesn't carry over. Opening a local store written by alpha.59 throws `UnsupportedStorageCodecsError` (match it with `isUnsupportedStorageCodecsError`) and leaves the store unchanged. Delete it, and the client resyncs from the server. Writes that were still waiting to upload when a device upgraded are lost, so let clients sync before you cut over.

- 473b1d7: Keep receiving acknowledgements and flow-control credits while the outgoing queue is full, so large write backlogs can finish syncing instead of stalling indefinitely.
- 7babede: `jazz-tools validate` now runs the checks a deploy runs on each migration, so a migration whose steps a deploy would refuse fails `validate` instead, before it ships. How the migrations join into one history is still checked at deploy, against the versions the store already runs. The refusal names the table and each column that doesn't come out as the schema says, and how (for example `"body" comes out optional but is required`), instead of "lens operations do not reproduce target columns".
- 5cf8292: `jazz-tools validate` now refuses a migration that still has a step its generated stub left to fill in, naming the file and line, instead of passing it to fail at deploy. Generated stubs fill a required column they add or drop from its declared `.default()`, so they leave such a step only where no default is declared. Policy and schema errors now read in schema terms: comparing `session.user` with a string column says it is the account's id and belongs in an `s.uuid()` column, and a join on a column that isn't declared as a reference says which table it should reference and how to declare it with `s.rel(...)`.
- 3ca94c7: `jazz-tools validate` now handles a project upgraded from alpha.59. It moves alpha.59's migrations to `migrations/legacy/` before loading anything, where it used to crash on their old migration DSL. It also moves snapshots that no remaining migration reaches to `migrations/legacy/snapshots/`, which fixes `createJazz`'s "disconnected roots" error after the upgrade. When no snapshot matches `schema.ts`, it keeps the newest one and tells you to run `jazz-tools migrations create` before the app starts.
- 760c30c: `vercel()` now refuses, at start, a Blob store that another environment's core already claimed: `createJazz` fails with "Blob store … serves the production environment, not preview; connect a separate Blob store to each environment". Before, a preview core given production's store started, acknowledged writes, and only logged a warning while it retried uploading their large values. A store connected through the API or CLI is connected to every environment, so give each environment its own.
- 45c3984: A `vercel()` core now keeps large values in the project's Vercel Blob store by default once one is connected (when `BLOB_STORE_ID` is set; a connection with only `BLOB_READ_WRITE_TOKEN` keeps SQLite). You can also choose it with `vercel({ blobs: vercelBlob() })`, or run `jazz-tools server --blobs vercel-blob`. Browsers send and fetch large values through the app's `/jazz` route as before. The core writes and reads the blobs itself, using the function's own OIDC token, which reaches it with each request. The Sandbox holds only the store's id. Blobs are private. Each one sits under `jazz/<store id>/` (claimed by one core) and is checked against its SHA-256 when read, because Vercel Blob keeps no checksum. A core without a Blob store keeps large values in its SQLite file and says how to connect one. It moves to the Blob store as soon as one is connected. `s3()` remains for buckets.
- d0f7053: Backend fixes under Next.js bundlers:
  - An app on `sqlite()` no longer needs `@vercel/sandbox` installed. Before, `next dev` and `next build` with Turbopack failed every `/jazz` request with "Can't resolve '@vercel/sandbox'".
  - `vercel()` finds its own version in a deployed function built with webpack (Next 15). Before, every request failed with "Cannot find module 'jazz-tools/package.json'".

- 0527e6d: vercel(): the Sandbox core now runs on the Node addon (`@garden-co/jazz-napi-linux-x64-gnu`) through its new `serveCore`, with the same connection and memory limits and graceful drain as before, instead of the `jazz-tools server` executable. `coreTarballUrl` overrides where the Sandbox downloads that addon from.
- b1768ee: On Vercel, `jazz.next()` now closes each sync WebSocket (1012, reconnect) shortly before its function's deadline, the route's `maxDuration`, preferring a moment with no traffic. Vercel ends the socket at that deadline and stops answering on it about 15 s before it closes; clients used to sit through that stall, and requests sent in it were lost. Now they reconnect at once.
- 31841a8: vercel() store fixes:
  - It now works when the app is built with webpack (Next 15). Before, every request failed with "Received an instance of URL".
  - A core that can't start is reported with the reason, for example a failed download from `coreTarballUrl`. When retrying can't help, such as a 404, its Sandbox is stopped so retries don't keep it awake. A temporary failure is retried as before.
  - A preview deployment without a Git branch, such as a CLI deploy outside a Git checkout or a deploy through the API, now fails to open and asks for `name`. Before, all such previews silently shared one core.

- a97cfcb: `vercel()` store: the function's Vercel OIDC token is the core's only credential. The core in the Sandbox admits only tokens naming the workload (team, project and environment) its first start pinned, keeps no secrets on its Drive (only the app id and that pin), and a deployment of another environment refuses to start or retarget it. Preview branches share one workload. `startLocalJazzServer` takes `trustedWorkload` for testing such a core.
- 9988640: Add the `vercel()` store for `createJazz`: the app's core runs in a persistent Vercel Sandbox in your own Vercel project, created on first use, woken on demand and stopped when idle. It authenticates with the function's OIDC token, so it needs no Jazz env vars, and runs a local SQLite core outside Vercel deployments.
- dea5aaa: A visible browser tab now notices when its shared worker dies, for example when another tab crashes, even if the tab has nothing pending. Before, such a tab stayed "connecting" until it wrote something, was hidden and shown again, or was reloaded, and the writes the dead worker still held waited for that too. Visible tabs now check the worker every 30 seconds and reopen it when it doesn't answer; hidden tabs still rest until they are shown.
- 2dbaa39: Vue's `<JazzProvider>` now takes its options in kebab-case as Vue templates write them. Before, `:clear-on-sign-out="true"` reached the session as `clear-on-sign-out` and was ignored, so signing out kept the account's local data. `clear-on-sign-out` and `link-guest-on-sign-up` are now declared props (a bare attribute counts as true), and any other attribute is camelized before it reaches the client.
- faa7249: Read the foreground transaction-time high-water asynchronously in the browser runtime. The synchronous WASM call blocked on the node lock while the lock holder waited for the JS event loop, so closing a foreground runtime could spin at full CPU instead of completing.
- 733ca26: A browser session now tries again on its own when a network blip, a timeout or a 5xx fails its check with your backend, or the sign-in that follows the check. It backs off from 2 seconds up to a minute. Before, the session stayed on the failure, often the error screen, until the page regained focus, so background tabs stayed broken.
- 31ba674: With `clearOnSignOut` and the local-first client (`client`), Jazz now also wipes a backend user's local replica when the page never saw them sign out. That covers a sign-out on your server, or another user signing in, followed by a reload. Before, the replica stayed in the browser's storage. On the next page load, once your backend names someone else or no one (asked twice, a couple of seconds apart), Jazz wipes it, unless another tab still has it open. This covers replicas opened from this version on.
- f211f68: With `clearOnSignOut`, a user signed out while no page of theirs was open (a server-side logout, or a switch to another user before a reload) is now wiped from IndexedDB on the next load. Before, the wipe reported success and left their local-first replica in place, because `deleteClientStorage()` on a browser Db that had not yet read or written did nothing.
- 89be08c: A `vercel()` core no longer goes on using an expired OIDC token for its blob store. It keeps the freshest verified token, and once that token expires it reports the expiry instead of sending the stale token. When a fresh token arrives, any blob store work that had backed off after failing starts again straight away instead of waiting up to a minute.
- a743d5b: Permission checks on writes now look up the rows their policies refer to through indexes instead of scanning the whole table. Before, each check read every row of each table its policy refers to, other apps' and tenants' rows included, so a write cost server CPU in proportion to everything stored. In BigLabel with 15,897 labels, one edit took 575 ms of server CPU.
- f4f4e37: Write denials are consistent wherever they are decided, and reveal less about whether a row exists. On the server, and on a backend with a local-only store, an update or delete a session may not make is refused with the same message whether the row is hidden from the session, deleted while hidden from it, or doesn't exist. An upsert can't hide this fully: on a row that doesn't exist it inserts, so it is refused only where the row exists. On a backend connected to Jazz Cloud, Cloud decides, and such an update is always refused with the same code, `permission_denied`, though not always with the same message or at the same time: a row the backend holds deleted is refused at once, a row it holds is staged and its `wait()` rejects with Cloud's refusal, and a row it doesn't hold is read from the server first (for up to 10 seconds) and then its `wait()` rejects. A write denied at once, as it is staged (on a local-only store, or a row the backend holds deleted), now throws an error with `code: "permission_denied"`, the code a rejected `wait()` carries, so one check handles both. `TestDb.expectDenied` now counts a write denied at once, and fails with "expected the write to be denied, but it was accepted" when it isn't denied.
- e61fd5b: Reject write-receipt waits on a different database node, even when its local
  sequence number matches. Receipts remain usable through schema views and after
  reopening their original durable node.
- Updated dependencies [0aec19c]
- Updated dependencies [fbbb242]
- Updated dependencies [ab06af5]
- Updated dependencies [ff0d69b]
- Updated dependencies [d09159b]
- Updated dependencies [70f3002]
- Updated dependencies [74f37fc]
- Updated dependencies [3ca94c7]
- Updated dependencies [7e56c40]
- Updated dependencies [f2a817e]
- Updated dependencies [6f292f9]
- Updated dependencies [d54110a]
- Updated dependencies [e432352]
- Updated dependencies [c510fd6]
- Updated dependencies [79b143e]
- Updated dependencies [ecb2595]
- Updated dependencies [68dd540]
- Updated dependencies [c584159]
- Updated dependencies [144326e]
- Updated dependencies [faa7249]
  - jazz-wasm@2.0.0-alpha.60
  - jazz-rn@2.0.0-alpha.60

## 2.0.0-alpha.59

### Patch Changes

- d06d76d: Name the table, row and versions that disagreed when a subscription's supporting-set transition is rejected, so an out-of-sync live query can be traced to the row that caused it.
- b420950: Deliver backend mutation errors to `db.onMutationError`, including rejections settled during startup replay before a listener was attached (bounded, with a dropped-count report), release listeners of dropped or shut-down backend `Db`s, and give wait rejections the same readable `reason` text as mutation-error events instead of Rust debug output.
- dd73564: Bound one-shot UUID page reads before policy evaluation, so a limited read no longer authorizes every candidate row first.
- 05dcbeb: The browser WASM is about 12% smaller (6.3 MB instead of 7.3 MB gzipped): JSON column schemas are now checked by a small JavaScript validator instead of a Rust one compiled into the WASM. A few rarely used JSON Schema features (`$dynamicRef`, draft-07 `contentMediaType`, and the `idn-email`, `idn-hostname`, `iri` and `iri-reference` formats) are reported as not supported in the browser yet, as are a few regular expression constructs the browser cannot match the same way (inline flags other than a leading `(?i)`, the `x` flag, possessive quantifiers, and a counted repetition directly after another quantifier). Code that loads `jazz-wasm` directly instead of through `jazz-tools` must call `setJsonSchemaValidator` before opening a database whose JSON columns declare a schema.
- f4431f0: Fix a client that stops syncing after it receives a streamed large value it did not have yet. When the value's content arrived while later updates that reference it were still being applied, the client could wait forever and stop sending and receiving until the page was reloaded.
- 846a02d: Keep clients that honour flow control inside the server's inbound frame budget, so fast imports are no longer disconnected with a backpressure error.
- 1002485: Prove writes allowed by `inherits`, join or `reachable` write policies from the sync server's own storage, without keeping a view of every row the policy matches for the life of the writer's connection.
- 28001f8: Exclusive transactions that were still waiting for the server when the app restarted now replay with their original read evidence, so they are accepted instead of being rejected as `exclusive_conflict`. A transaction whose data really changed meanwhile is still rejected, and transactions written by older versions still behave as before.
- 51bbac0: Fix browser subscriptions that silently stopped delivering while a backend rapidly rewrote rows next to a large (chunked) value. Persisting a publication now keeps driving the query work that owns an in-flight chunk install, so the SharedWorker no longer deadlocks on IndexedDB's write gate, and direct query openings no longer take over the runtime owner's wake-up for suspended cold work.
- 4131d0d: Keep subscriptions that select only small columns fast beside large values: a listing no longer downloads, or waits for, the large columns it does not select, so a client joining next to a multi-megabyte file sees its rows promptly.
- 617dd85: Local-first clients now keep their connection for as long as they run. Token refreshes keep working after the first one, a client whose token has expired renews it and reconnects on its own, and an account context that fails to refresh logs the error and tries again.
- 1db0aaf: Reject non-finite numeric schema defaults before JSON serialization can silently convert them to `null`.
- 05cb615: Nullable columns with ordinary filter operators now expose `isNull` in TypeScript filters, including scalar, byte, and array columns. Byte and array `isNull` conditions lower without attempting to convert the boolean flag as a column value.
- 36d7f66: Keep local writes visible while a query waits for large-value bytes from the server. A write no longer waits for another query's first result that is stuck on an attachment fetch, for example while offline. That query restarts after the write and keeps its pending fetch.
- 2916d59: Fix duplicate and lost query results when a write arrives while an earlier write is still waiting for large-value content. The later write now sees the earlier write's finished query state. Before, a joined row could be reported twice, and a reference from the later write could be dropped.
- c9dd40e: Permission advice for reads, deletes and the `using` clause of updates now proves only the target row instead of re-evaluating the policy across the table, which makes permissioned writes on large tables much faster.
- 9a6d63a: Compile policies that are an AND of ORs without expanding every combination: a policy like `(a OR b) AND (c OR d) AND …` is now checked as one union per OR instead of one branch per combination, so compile time grows with the number of ORs rather than doubling with each one.
- 2abbd81: Compile write-policy support views with many policy branches faster: lowering no longer copies the table schema (with every policy branch) into each step, derives the parameter domain once per query instead of once per branch, and derives each graph node's output fields once. Chains of policy `exists` joins now carry only the columns later joins and the result read, instead of every column of every joined table.
- d1b3a7d: Stage inspector overlay assets before replacing the published copy, preserving the existing overlay if staging or promotion fails.
- 734117f: Lower a sync server's peak memory while it serves a large query to a fresh client. The first result is no longer copied before it is sent, is encoded once at its exact size, and no longer holds oversized query buffers.
- a0fa680: Clients now reconnect with backoff when the server ends a connection because it is overloaded or still starting, and resend their pending writes. Previously browser and Node clients stopped syncing until the app called `reconnect()`.
- 25db6a0: `resolveRequestSession` now throws a `RequestAuthenticationError` (exported from `jazz-tools/backend`, with `isRequestAuthenticationError` to match it across bundled copies) when the request's credential is missing, malformed, unverifiable, expired, for another issuer or audience, signed with an algorithm the configured `jwtPublicKey` does not support, or refused by the account registry with one of its rejection codes. Server-side failures, such as an unusable `jwksUrl` or `jwtPublicKey`, a JWKS or registry outage, a registry answer without a rejection code (such as a bare 404 for a wrong app id or registry URL), or a malformed registry answer, stay plain errors, so routes can answer 401 only for the former. A JWKS refresh that fails after a bad signature is now reported as the fetch failure instead of as an invalid JWT. The configured `jwtPublicKey` is now imported once for every algorithm it supports.
- 708c0f9: Retry a first server connection that fails at the network layer, so a local-first client opened while offline syncs its writes once the network returns. A server URL that cannot be reached now reports its transport error after 7.5 s of retrying instead of immediately.
- 8175adf: Prove writes allowed by row-only write policies without hydrating every row the policy matches, so a sync server's memory no longer grows with each accepted client write.
- fdc9143: A permission policy's `exists` now sees the rows the same transaction inserts or updates when the server checks it. Creating a row and the rows whose permissions depend on it in one `db.transaction(...)` or exclusive transaction is accepted instead of rejected with `permission_denied`. A committed row the transaction deletes still satisfies `exists`; a row the transaction both inserts and deletes, and writes from other transactions that are not accepted yet, never do. Every write must also pass against the state the whole transaction leaves behind, so a transaction that removes the access one of its own writes relies on is rejected. A transaction whose rows mostly depend on each other, and whose checks would read more than 262,144 of its own rows, is rejected with `write_rejected` and a reason ending in "is not supported yet"; imports that rest on existing rows are checked once per row.
- 71e871a: The Linux `jazz-tools server` binary samples its heap: `GET /debug/pprof/heap` (admin secret, or the new token-protected `--diagnostics-listen` listener) returns a pprof profile of in-use memory with function names and the binary's build ID. `JAZZ_HEAP_PROFILE_SAMPLE_BYTES` changes the mean bytes between samples (512 KiB by default); `0` turns sampling off, and the route then answers `404`.
- 2aad1da: Compile a session's write-support views only over the policy alternatives its claims can satisfy, which cuts first-write latency for role-guarded policies.
- c216596: A backend `update()` inside `forRequest`/`forSession` on a row this backend instance has not loaded yet (for example right after a restart with an empty data dir) now fails with a clear `not_observed` error ("... row ... is not loaded locally; read or subscribe to the row before updating it") instead of the misleading "read policy denied UPDATE ... requires read permission on the target row". Rows the backend holds but the session may not read are still denied as before.
- 7cf2f21: Fix the browser runtime stalling forever under webpack 5 (for example `next dev --webpack`). The broker SharedWorker was constructed from two separate `new SharedWorker(new URL(...))` call sites, so webpack emitted two worker chunks and the foreground lease and the runtime connection ended up in different SharedWorker realms: the runtime worker never loaded WASM or opened its WebSocket, and queries stayed pending. Both now go through a single construction site, so every bundler emits one broker worker.
- a4880f7: Keep read-your-writes for Global reads during large-value uploads: a Global read now waits until the local writes it depends on (writes to the tables it reads, including a streamed row itself) have gone out to the server, and fails with an explicit error if one of those uploads fails. Reads of other tables are not delayed, and the wait no longer counts against the read's coverage timeout.
- 4b9dffe: `tx.update(..., { applyDiffs })` now works inside mergeable and exclusive transactions. Diffs address the transaction's own view of the row, so repeated appends compose, and each one rewrites only the pages it touches, so streaming into a long text value in transactions stays cheap as it grows.
- eaceec7: Transactions now read other nodes' unsynced writes that plain reads already return, including a previous node's writes after a runtime reopens as a new node.
- 6b03d94: Let Next.js apps import `jazz-tools/backend` statically under Turbopack when `jazz-napi` is a workspace link. Turbopack ignores `serverExternalPackages` for packages that resolve outside `node_modules`, so `withJazz` now aliases a workspace-linked `jazz-napi` to a module that loads it through Node at runtime.
- Updated dependencies [d06d76d]
- Updated dependencies [d16b569]
- Updated dependencies [05dcbeb]
- Updated dependencies [c93763e]
- Updated dependencies [36d7f66]
- Updated dependencies [6eebb2c]
- Updated dependencies [bb22c51]
  - jazz-wasm@2.0.0-alpha.59
  - jazz-rn@2.0.0-alpha.59

## 2.0.0-alpha.58

### Patch Changes

- dbd893b: Reads inside transactions on a `Db` from `withAttribution` or `withAttributionForRequest` now use the backend authority that the attributed `Db` reads and writes with. They no longer fail with "open transaction identity does not match its bound identity", so the invite-link recipe works as written.
- 238d0b8: A local read's background refresh can no longer break the server connection when it fails (for example on a query-coverage timeout). Pending global waits on that client no longer fail with the refresh's error, and the client still reconnects after a later network drop instead of staying offline.
- 26119fe: Report already-connected migrations as skipped with their schema hashes without implying a migration was published or skipping the deploy.
- 25c1bb2: Resolve plain-record request authorization headers case-insensitively and reject conflicting case variants.
- 21ba9d5: Reduce memory use and CPU time when reading large values by checking each stored chunk's canonical encoding in place instead of rebuilding a temporary copy of it.
- 2d1d32e: Speed up reading large values (files, long text and JSON) by no longer re-hashing chunk bytes that the chunk provider has already verified.
- b7cdad1: Compile policies that reuse subgraphs faster: lowering walks each shared subgraph once instead of once per path to it.
- 0fc1c89: Closing a transport while an operation on the same runtime is suspended no longer aborts the process. The connection is detached on the next tick instead.
- 3e4f819: Fix writes failing with `graph field not found: __jazz_claim_typed…` when permissions are published with `deploy` and an insert checks a table whose read policy compares a session claim. Deployed permissions now behave like the same permissions supplied at server startup.
- fbd68bd: Document the explicit `s.rel` / `s.reverse` relation API in the `jazz-tools` README, list every starter and the `--hosting` values in the `create-jazz` README, and make `PolicyTestApp.seed` throw a descriptive error when its callback returns something other than a write result.
- aef96a5: Add Effect v4 bindings: `jazz-tools/effect` provides Jazz as an Effect service with live queries as Streams and transactions that commit or roll back with their effect, and `jazz-tools/effect/backend` provides it per request on Node backends.
- 3a378d3: Queries that compare an array column with an empty array literal are now accepted by query validation.
- 46f8ccc: Inserting a row with every column omitted (for example `db.insert(table, {})`) now creates the row with null values when the table has at least one optional non-JSON column, instead of failing with "mergeable commits must carry content cells". Tables whose only columns are optional JSON still can't take such an insert; it now fails with an explicit error until optional JSON columns can store null.
- 7723921: An exclusive transaction that reads with `include()` or a join no longer times out with `not_observed` (or hangs in the browser) when the related table has a read policy that uses the session, such as a `$createdBy` owner check or access inherited from another table.
- 84e23fe: Exclusive transactions that read through a join, an include or a related list now record only the related rows they could have used. They no longer download every related row the reader can see, and a write to an unrelated row in the related table no longer makes them conflict.

  Reads whose related rows can't be narrowed this way (lookup joins, flat joins, unions, recursive traversals, inherited access, or deleted rows together with related rows) now fail inside an exclusive transaction with an error such as "Reading a flat join of `invites` with `members` is not supported in exclusive transactions yet", instead of reading the whole related table.

- f572ab0: Exclusive transactions no longer act on stale data. The server now checks an exclusive transaction against the rows it actually read, including counts and other aggregates, and rejects the commit with `exclusive_conflict` if any of them was deleted or changed, or if a row now matches one of its queries that it did not see (for example someone else's redemption of a single-use invite). A revoked invite can no longer be redeemed.

  Exclusive transactions can now be prepared offline: reads answer from local data, the commit is stored locally, and the server accepts it once it syncs if everything the transaction read still holds.

  **Upgrade clients and backends that use exclusive transactions.** Only alpha.58 clients send the row records the server checks, so a backend that redeems invites, like the invite links recipe, is protected only once it runs alpha.58. An exclusive transaction from an older client is rejected with `exclusive_conflict`, on every attempt until the client is upgraded, whenever one of its reads (a query, a read by id, a table read or a count) matches rows on the server, and whenever it updates or upserts a row in a table that holds other rows the writer can read. An older client's read is checked only against rows that still exist, so a read of a row that was deleted since, such as a revoked invite, can still commit. Inserts and deletes without reads still commit.

  Exclusive conflicts rely on the writing client reporting its reads, so they are not a security boundary. Enforce rules every writer must follow in permission policies, or write on your backend.

- 1ee1150: A live subscription waiting on cold storage no longer keeps graph garbage collection pending, so later polls stop repeating collection work that cannot reclaim anything.
- bdba078: Fix session reads with recursive or gather permission policies failing with "graph contains a dependency cycle" in release builds (NAPI and WASM) when one query input is reached through two paths.
- 6f3f432: `include()` now delivers rows whose nullable JSON column (`s.json().optional()`) is unset. Previously the read timed out with "Timed out waiting for query coverage" and subscriptions never called back, in both include directions.
- c01f504: Inspector `IN` / `NOT IN` filters split values only on top-level commas, so JSON values that contain commas are kept intact.
- 94552bc: Make appending to or editing a large text or bytes value cost proportional to the edit rather than the whole value, by validating only the new part of a locally derived value and no longer rebuilding the value to find the row's current version.
- 2a2dfeb: In-memory databases now honour their storage's eager read retry setting, which lets a read that yields once be re-polled in the same turn.
- ef56954: Fix `jazz-tools migrations create` recovering a stale lock by taking a live one. When several generators ran against the same migrations directory, one could mistake a freshly acquired lock for the dead owner it had just read, move it aside and fail with "Quarantined migration lock owner did not match", letting two generators run at once. Stale-lock recovery now only ever removes the exact lock instance it observed.
- f04f7d2: Disable Nagle (TCP_NODELAY) on native Node and React Native client WebSocket connections so small sync frames are not delayed by the server's delayed ACK.
- 94e3090: Keep native client relay ticks recoverable after temporary socket closure and HTTP connection failures (408, 425, 429, 500, 502, 503, 504), as well as hostname-resolution failures or TLS EOF without `close_notify`. Authentication, malformed protocol, certificate, and unclassified I/O failures remain terminal.
- 99e2dab: Errors thrown from the native Jazz runtime now carry a stable `code` (for example `not_observed`); in the browser they are now `Error` objects instead of bare strings. Messages are unchanged. React Native: the native relay ABI moves to 2, so a JavaScript-only (OTA) update to this release needs a new native build; a mismatched build fails at startup with "new native development/release build required".
- 22ec02f: A server pump failure while a native read is waiting now surfaces as a transport error instead of an unhandled promise rejection that could crash Node.
- 3296dfd: Retained hydration memo lookup and lifecycle cleanup now use a node-scoped key index, avoiding scans across unrelated cached memo entries. Memo reuse, invalidation, eviction, and byte accounting are unchanged.
- 08605c7: Writing a value to an optional JSON column (`s.json().optional()`) from TypeScript now works. Previously every insert or update that set such a column failed with `value does not match type Internal(InternalValueType(StoredScalar(Json)))`. Writing `null` to it is still tracked in #2733.
- b1d93bf: Writes into large ordered subscriptions no longer slow down as the result grows: snapshot positions now update incrementally instead of being rebuilt on every change.
- ce8372d: Permission policies can compare an optional column with a required column of the same type inside `exists.where(...)` (for example an optional invite code against a required one). Publishing such policies no longer fails with `OperandTypeMismatch`; a `null` value still never matches.

  [PR #3732](https://github.com/garden-co/jazz/pull/3732).

- 028a292: Maintained subscriptions that look up a row by id on a table used by permission policies now read that row directly instead of scanning the table.
- 6a88729: `createPolicyTestApp().as()` can now act as a local-first guest: pass `authMode: "local-first"` without an `issuer` and the test app acts with a real self-signed local-first identity and its founding account, admitted the same way `forRequest()` admits local-first clients. Sessions with an explicit external `issuer` keep their existing behaviour. `testApp.accountFor(session)` returns that account. A missing author issuer or subject now fails with a clear error instead of a `TypeError`.
- c127b57: Variable-length array fields in stored records now reject offset tables whose arithmetic would overflow, instead of wrapping. Encoded bytes are unchanged.
- 8b0ed75: Keep large-value chunk installs progressing while a durable relay writes its repair ledger, so a relay no longer stalls when a ledger write waits on a chunk install that holds the storage writer.
- 60696e8: Node and React Native dev runtimes no longer fail to reopen local storage when a schema adds tables: reopening a RocksDB or SQLite store to add column families now keeps the store's codec profile instead of failing the pinned-profile check.
- 230e2d5: Session-scoped writes on a backend (`forSession()`, `forRequest()`, and `createPolicyTestApp`) no longer abort the process with "reentered a suspended operation" when they overlap an earlier write that is still in flight, such as two concurrent upserts of the same row. The session's claims now wait behind the earlier write and take effect in the order the writes were made.
- 9cb0889: Skip rewriting storage when a sync server re-delivers an already-applied acceptance receipt for a complete mergeable transaction, making reconnects with many settled writes cheaper.
- 9c45b8b: Fix `db.transaction` updates on rows that hold a large text or bytes value in another column. Updating one column inside a transaction no longer fails with "callers must author logical scalar values, not physical large descriptors"; untouched large values are kept unchanged, and reads inside the transaction see the update.
- f125333: Report a local exclusive transaction conflict as a `PersistedWriteRejectedError` with code `transaction_conflict`, instead of a plain `Error`, so apps can handle it with the same `instanceof` check as an authority rejection.
- e3e5766: Updating a row that this client has not loaded yet now fails with a clear `not_observed` error ("... is not loaded locally; read or subscribe to the row before updating it") instead of a misleading "read policy denied UPDATE ... requires read permission on the target row", which was reported even when the table's read rule is `always()`. Rows the policy genuinely hides are still denied.
- d883cf4: In the browser, loading Jazz no longer fetches and decodes the same WASM file again when the account manager and the database both initialize it.
- 20666e3: Reject an unknown `wait({ tier })` value (for example a typo from plain JavaScript) with a clear `TypeError` that says the write was already applied, instead of a native "unknown durability tier" rejection that looks like a rejected write.
- Updated dependencies [6318998]
- Updated dependencies [94e3090]
  - jazz-rn@2.0.0-alpha.58
  - jazz-wasm@2.0.0-alpha.58

## 2.0.0-alpha.57

### Patch Changes

- d506141: Resolve a stored version's writer node in constant time instead of scanning every node the server has seen. Servers with many client nodes no longer slow down per write or open in quadratic time.
- 146859d: Commits, ingest validation and row reads no longer copy a table's schema, including its policy definitions, for each row they touch. Results are unchanged.
- 6b2cba3: Wire the Inspector overlay into the SvelteKit development integration and dispose managed runtime state when the plugin closes. The SvelteKit and Vite dev plugins now keep the managed Jazz server running across Vite dev-server restarts (`.env` or `vite.config.ts` changes), and stop it only when the last dev server closes.
- dbbdd23: Preserve managed development-server storage and auth options, and add a `server.host` option that binds and advertises an explicitly configured IP address (for example a LAN IP reachable from an Expo device).
- 54b7172: Serialize automatic local-first startup so first-visit browser identity adoption converges on one durable root across tabs.
- 163799e: Make subscription deltas cheaper in the JS client: large removals in one delta, long-running windows (the per-delta cost no longer grows over time), subscriptions with `include(...)`, first-load result keys and native row decoding. Each delta also records an undo log instead of snapshotting the whole result, and the ordered-id index stays lazy during runs of sequential changes; the deltas delivered to listeners are unchanged. `decodeNativeRow(...).valuesByColumn.get(name)` now returns the same object as the matching entry in `values`.
- 5cc6c78: Queries are compiled once per shape and bound to their parameters, so many subscriptions of the same query with different parameters reuse one compiled program and plan. Opening each extra subscription is much cheaper, and inserts and updates do less work.
- 1f58c62: Use declared composite indexes for Global-tier reads. With an `(owner, rank)` index, a page filtered by `owner` and ordered by `rank` with a `limit` reads only about `limit + 1` index entries and checks deletions for those candidates only, falling back to the ordinary read when it cannot prove the page complete. A first result that filters both columns of a two-column composite reads that index prefix. Results are unchanged, Local-tier reads keep their current path, and schemas without a composite index take none of the new paths.
- 163799e: Declare ordered multi-column indexes with `s.table({...}).compositeIndex(["owner", "rank"])` in TypeScript or `TableSchemaBuilder::composite_index` in Rust. An index needs at least two distinct declared columns, and `bytea` columns cannot be indexed. The index is stored and backfilled when the schema is published through the usual migration. Declaring a composite index changes the schema id and needs a server on this version: `deploy` and `pushSchema` stop with `SchemaHashMismatchError` against an older server, which will already have stored the schema without the index; no migrations or permissions are published. A store that uses composite indexes cannot go back to an earlier version. Schemas without a composite index keep byte-identical ids and hashes.
- 3245522: Finding a row's current version now walks its edit history once instead of once per version, so reading or committing a row that has been edited many times (such as a counter or balance) no longer slows down quadratically with its history. Results are unchanged.
- 163799e: Fix a self-hosted Core server in dynamic-schema mode crashing on every restart with `opened schema is absent from the durable catalogue`. This happened when the newest schema was published without a lens, or its lens bridge failed. The server now reopens with the newest schema its store already holds and never admits the unbridged schema.
- fb46c59: Opening many subscriptions at once, such as a whole dashboard, no longer recompiles queries whose compiled programs were evicted before their installers ran, and the engine classifies each query node's aggregate dependencies once instead of walking its ancestors every time. Results are unchanged.
- 5b43547: Mark `wait({ tier: "edge" })` as deprecated in the TypeScript types instead of rejecting it at compile time. It still waits for `"global"`, and editors now point to `"global"` as the replacement.
- 17e4b95: Each runtime tick no longer scans all cached evaluation and operator state, so a small write stays cheap as the number of live queries and cached results grows. Results are unchanged.
- cf5f6c3: In an exclusive transaction, `insert` with an explicit row id is now create-only: it rejects with `WriteRejected` instead of replacing a row that already exists, is deleted, or was already written for that table earlier in the same transaction. Use `upsert` (or `update`) to overwrite. An insert over a row hidden from the caller by read policy returns the same error as an `upsert` over it, so the rejection does not reveal that the row exists.
- e90238f: Avoid rejecting exclusive single-table predicate reads when unrelated local writes leave their result unchanged. Matching local or remote phantoms still reject, and relational, whole-table and aggregate reads retain their existing conservative local conflict checks.
- 163799e: Answer first one-shot reads with fewer row loads: point joins by an exact id use the junction's foreign-key index, root and join equality filters are pushed into indexes, and two equality filters intersect their index keys before rows are loaded. Live subscriptions are unchanged.
- 8cb386f: Apply small subscription deltas row by row in the Vue, Svelte and Solid adapters instead of re-merging the whole result list, so untouched rows keep their object identity and receive no reactive writes. Frames with many changes still reconcile the whole list in one pass. The exported `applyDelta` no longer repairs a target array that was edited outside Jazz; call `reconcileArray(target, all)` for a full repair.
- e868cad: A Global wait or read issued right after a short server outage no longer rejects with a stale "websocket closed" error while the client is about to reconnect. The outage is now published only when a reconnect attempt fails after the link has been down for 7.5 s, and retries run at up to 1 s intervals until then.
- f12b131: Fix intermittent browser startup failures with `IDBTree page … exceeds the configured page size` (PageTooLarge). When an IndexedDB B-tree page holding entries of very different sizes overflowed, it was split by entry count, which could leave one half still too large. Pages now split at a byte-balanced boundary where both halves fit. The IndexedDB storage format is unchanged.
- 8cb386f: Fix a browser write cancelled while its IndexedDB commit was pending (for example by a torn-down page or a cancelled task). Reads could return the unsaved rows, and every later write failed until the page reloaded. Storage now reloads from IndexedDB on the next operation, so only writes IndexedDB confirmed are visible and later writes work normally. The IndexedDB page format is unchanged.
- 163799e: Speed up IndexedDB storage. A cold scan in a fresh tab or worker reads one IndexedDB batch per tree level instead of one per page, and large write batches edit pages they already own in place instead of copying them. The page format is unchanged.
- 163799e: In the browser, reads of data already in memory no longer wait while a write commits to IndexedDB. Outside a transaction they return the last committed state: the in-flight write becomes visible once its commit succeeds, and a failed commit never does. Inside a transaction, reads still see the transaction's own writes. Reads that need data not yet in memory still wait for the commit. The IndexedDB page format is unchanged.
- 7edab06: Queries that join through a link table with a two-column composite index on that table (for example `[linkColumn, targetColumn]`) now skip link rows whose target is outside the root's indexed prefix, instead of hydrating every link row first. Results are unchanged.
- a231d2d: Require declared JSON columns for JSON pointer reads, reject noncanonical nested array indices, and preserve demand-driven reads of validated JSON values.
- 98b7b6a: Range reads of large values and queries that project large columns away no longer load whole large values, so their cost no longer grows with value size.
- 17e4b95: Opening a subscription over a table's rows now takes time linear in the number of rows instead of quadratic. Results are unchanged.
- f9612e2: **Breaking:** replace `ReadTier.RemoteIfPossible` (`"remote-if-possible"`) with `ReadTier.LocalFirstUnlessEmpty` (`"local-first-unless-empty"`).

  The new tier serves local-first results immediately and keeps syncing, like `ReadTier.LocalFirst`. The only difference is an empty local result: while the server is reachable, or its first connection is still being set up, the first delivery waits for the server's first answer instead of showing an empty state. A fresh device therefore opens on synced data, and a device with cached data opens instantly.

  The gate lives in the core database, so browser, Node, React Native and Rust clients share one behavior. It never waits on a server it cannot reach. An empty result is delivered at once when no server is configured, after `db.disconnect()`, while the connection is down or retrying, or when the connection attempt fails. It is also delivered when the connection drops during the wait, or five seconds after a connection attempt started if it has not come up. Queries with an `offset` read the server's page whenever the server can answer, because a partly synced cache would apply the offset to the wrong rows. When the server cannot answer, they show the cached page.

  `ReadTier.RemoteIfPossible` and `"remote-if-possible"` are removed, in TypeScript and in Rust (`ReadTier::RemoteIfPossible`); passing the old name throws an error. It waited for the server whenever the app had not called `db.disconnect()`, so a dropped or never-established connection left it waiting, or failing, instead of showing local data. Switch to `ReadTier.LocalFirstUnlessEmpty`, or to `ReadTier.Remote` for server-confirmed results.

- fb46c59: Local-tier client subscriptions of the same query with different parameters now share one prepared query shape once a second binding opens, so each write is evaluated once for all of them instead of once per subscription. A lone subscription, or reopening the same binding, keeps its own plan as before. Results are unchanged.
- a0c4679: Keep driving a Node client tick that yields between evaluation turns instead of dropping it. A fresh subscription whose first result was a few MB (for example 100 rows of 60 KB) previously never delivered any rows and reported no error.
- fb46c59: Fix the native Rust `JazzClient` panicking with `synchronous node operation … reentered a suspended operation` when a query, subscription or transaction read started while the client was waiting on storage or large-value chunks. It now waits for that operation to yield and then runs. The NAPI, WASM and React Native bindings were not affected.
- 18544b1: Fix the React Native relay dropping outbound sync messages when its socket was backpressured. Unsent messages now stay queued in order and are retried, and the socket worker keeps running instead of stopping.
- 8af99ba: Fix one-shot reads (`db.all`, `db.one`) without includes that order by a column they don't `select`: rows now come back in that column's order instead of row-id order, including for `limit`/`offset` pages. Subscriptions were already ordered correctly. Reads with `.include(...)` still ignore such an ordering (#3503).
- 67c0fca: Fix includes ordered (or sliced) by a column that is not selected. Such reads previously failed locally with `graph output descriptors do not match`, and remote reads and subscriptions were rejected by the server and never settled.
- 3d691af: A write to a table read by an ordered subscription (`orderBy`) no longer costs time proportional to the whole ordered result. The engine now tracks row positions only for outputs that can use them. Results are unchanged.
- 2a30ac5: Fix a server failing to sync a subscription over roughly 100k rows with `semantic message exceeds routed payload limit`. When a view update would exceed the 256 MiB message limit, the server now leaves some row bodies out and the client fetches them before applying the update in one piece. Updates under the limit are sent unchanged.
- d5abed2: Keep reconnecting to the sync server after an outage of any length. Previously, clients gave up after 10 failed attempts (about 8 seconds) and never synced again until the page was reloaded or the process restarted. They now retry indefinitely with capped, jittered exponential backoff, and queued writes sync once the server is back. Global reads and waits still reject once the server has been unreachable for about 8 seconds, and they work again after the reconnect.
- c9ca602: Relation queries whose projection renames or narrows columns now return exactly the projected columns under the requested aliases, with each source column's type and nullability, on both one-shot and live reads, instead of whole source rows. Queries whose projection is the table's full column set (as `hopTo(...)` and payload `match` queries produce) keep returning ordinary rows and keep supporting `include(...)` and `select(...)`. Combining `include(...)` or `select(...)` with a renaming or narrowing projection, ordering a `union(...)` by a column outside its output table or by `$createdBy`/`$updatedBy`, and using an internal name (`row_uuid`, `tx_time`, `tx_node_id`, or a `$`, `_app_`, `__`, `left.` or `right.` prefix) as an output alias in a renaming projection or in any `union(...)`, including a `union(...)` over a table with such a column, are now rejected with a clear error.
- d3d8ba5: Remove server edges. Clients connect to Core, which authorizes reads and writes and confirms global durability. Browser workers and native local persistence relays remain supported, as do local queries and optimistic writes.

  For applications using the old durability options:
  - Replace write waits using `{ tier: "edge" }` with `{ tier: "global" }` when the write must reach the server. Keep `{ tier: "local" }` for local persistence.
  - Use `ReadTier.Remote` for reads that need Core confirmation, or `ReadTier.LocalFirst` for immediate local reads.
  - Remove server upstream/edge configuration. A server now runs Core; a connection gateway may still route traffic without running a Jazz database.

  Existing globally confirmed data keeps its storage encoding. Legacy edge durability is interpreted as local persistence. Locally authored edits accepted only by an old edge remain eligible for normal resubmission to Core, with their original authorship and transaction identity; that old acceptance does not bypass current write permissions. Do not clear local databases to migrate.

  For a historical semantic-edge store, an edit can remain pending if Core lacks a parent created by another author or by the old edge. That parent must arrive through an authorized recovery path; reconnecting as the child’s author does not grant authority to upload someone else’s work. Keep the local store intact. Recovery for such shared-edge histories is tracked in [#3242](https://github.com/garden-co/jazz/issues/3242).

- 1100fdc: Handle the tiers removed in alpha.57 at runtime. `wait({ tier: "edge" })` on an already applied write now waits for `"global"` and warns once, instead of rejecting after the write committed (which made retrying callers duplicate rows). `db.all`, `db.one`, `db.subscribe`, transaction reads and `useAll` now reject `"remote-if-possible"` and `"edge"` read tiers with an error naming the replacement, instead of silently running at the default tier.
- 5cc6c78: Fix a client staying on the slower initial-sync flush cadence after a reconnect or resubscribe in which every referenced payload was already stored locally.
- 88f3c46: React Native writes no longer block the JS thread on applying the write, subscription (IVM) evaluation or relay pumping: the native relay owner does that work on its own thread and wakes JS with already-computed results. Write errors now surface the same way on React Native, web and Node. Validation errors, a closed Db and an unknown table throw synchronously. Failures that depend on row state are reported through the write handle: updating, upserting or deleting a row that is already deleted no longer throws on React Native; the returned handle's `wait()` rejects with `row already deleted: <id>`, and `onMutationError` fires if nothing is waiting. Direct writes are bounded at 64 queued per client; when that queue cannot make progress, the call throws a synchronous `backpressure` error and admits nothing. Reads and subscriptions opened after a write in the same turn reflect it.
- 4bd18f2: A root server no longer keeps a copy of every client write in its upload queue for the life of the process. Queued uploads and downstream fates also drain in linear time after a long offline period.
- 5cc6c78: Fix nested data that stopped updating after a subscription delta removed a root row and added it back in the same frame (for example, a joined or `include` result that re-sorted). The client kept the row in the result but dropped its nested state, so later changes to its children were never applied. This also affected alpha.56.
- e0e92a9: Enable TCP_NODELAY on accepted server connections: the standalone `jazz-tools server`, the `jazz-server` loopback WebSocket binary, and the embedded server. This avoids a Nagle/delayed-ACK stall between small multiplexed WebSocket writes, which can add roughly 40ms to remote reads on Linux after the alpha.56 transport update. Wire and storage formats are unchanged.
- 17e4b95: Account-session websocket connections no longer look up the session registry on every incoming message. They re-check only when the registry changes, so servers with many open connections spend much less time per message. Session revocation still takes effect.
- 163799e: Evaluate each uploaded row's write policy once instead of twice when a server admits a client session upload, stop re-encoding and re-validating uploads that were already checked, and open an existing RocksDB store once instead of twice at startup.
- 163799e: Make writes cheaper when many subscriptions share one query shape with different parameters. A write now only evaluates the subscriptions whose parameters it touches, writes to tables no subscription reads no longer scale with the number of live subscriptions, and one-shot queries and unsubscribes no longer walk the whole subscription graph. Attaching another subscription to a query shape that is already live is incremental, so it costs about the same as the first instead of growing with the number of subscriptions already open.
- 75de6bc: Transactional reads and commit validation decide whether a row version is visible from its transaction's global time and fate alone, instead of decoding the whole stored transaction for each past edit. Point reads and commits on frequently edited rows are faster. Results are unchanged.
- 17e4b95: Inserts, queries and subscription refreshes no longer rebuild a table's physical schema and record descriptor each time they build a source graph, which makes seeding and writing to large stores faster. Results are unchanged.
- 515bac3: Fix fresh subscriptions that never delivered a first result once the table held a row with a value over 64 KiB. The client no longer holds its sync turn open for large-value chunks that only that turn can request, and it waits for those chunks before reporting a settled result.
- 5cc6c78: Fix query nodes leaking when a subscription was released while its first results were still loading from storage. A subscription opened on the same data during that window could also lose nodes it still needed. Both now keep exactly the nodes that live subscriptions use.
- ac4c4e1: Servers measure an outgoing sync message's size without encoding it a second time, and fan-out to subscribers no longer copies each update for the last recipient. This mainly saves memory when opening large subscriptions. The wire format is unchanged.
- 839290a: Speed up include-subscription child edits: native bindings hand terminal-operation bytes to JavaScript as `Uint8Array`s, React Native decodes them without a per-byte round trip, and large multi-table sync streams no longer re-decode row descriptors. `jazz-napi`'s `SubscriptionTerminalOperation`, `KeyPathSegment` and terminal edit types now carry keys and values as `Uint8Array` instead of `Array<number>`.
- 146859d: Debug trace environment variables (`JAZZ_COVERED_INPUT_TRACE`, `JAZZ_REHYDRATE_TRACE`, `JAZZ_QUERY_TEMPLATE_TRACE` and `JAZZ_FORCE_SINGLETON_VERSION_CARRIERS`) are now read once per process instead of on every send and delta, so set them before the process starts. Refreshes also match changed storage tables to schema tables without formatting every candidate name.
- 17e4b95: Queries inside a transaction read each table's history once instead of re-scanning every row's history, so each stored version is decoded once rather than three times. Transactional reads and checkouts are noticeably faster. Results are unchanged.
- 17e4b95: Answer update permission advice for tables without an update policy with a point lookup instead of scanning the table. Results are unchanged.
- 258b350: Keep repeated one-shot reads of the same query fast now that subscriptions attach to already-live query shapes. A read that follows an unsubscribe of the same shape no longer runs an extra retraction pass before hydrating, so warm one-shot reads cost about the same as before that change. Results are unchanged.
- b554916: Compressing a small sync message no longer allocates a buffer the size of a full frame, and incoming compressed messages decode straight into their reassembly buffer instead of through temporary copies. The wire format is unchanged.
- Updated dependencies [1f58c62]
- Updated dependencies [163799e]
- Updated dependencies [163799e]
- Updated dependencies [f12b131]
- Updated dependencies [8cb386f]
- Updated dependencies [163799e]
- Updated dependencies [163799e]
- Updated dependencies [d3d8ba5]
- Updated dependencies [163799e]
- Updated dependencies [b554916]
  - jazz-wasm@2.0.0-alpha.57
  - jazz-rn@2.0.0-alpha.57

## 2.0.0-alpha.56

### Patch Changes

- 87ae159: Wake queued publications and fail subsequent writes when an abandoned predecessor breaks publication order, preventing successors from waiting indefinitely.
- 2e227c5: Avoid building a retained ArgMin/ArgMax candidate index for one-shot snapshot probes. Subscription hydration still seeds incremental state, including when a probe populated the shared result cache first.
- 55f5d3b: Fix synchronization failures on optimized ARM64 builds by preserving the correct physical and decoded-message credit classes.
- e43d5c1: Bound account registry and backend JWKS requests, including response bodies, to thirty seconds so stalled authentication cannot indefinitely block session teardown. Failed JWKS downloads remain immediately retryable without extending cached key trust.
- 7f85f30: Fix subscriptions prepared before the first server catalogue arrives retaining temporary table identities. After adopting the server's identities, Jazz now refreshes the affected sync metadata while preserving pending writes and existing local query subscriptions, preventing intermittent permission-scoped reads from being rejected or remaining stuck during startup.
- 05717bd: Order nullable Better Auth sort values consistently while preserving stable pagination.
- 294d130: Preserve Better Auth custom stored-field mappings across adapter reads, sorting, and supported query paths.
- 45fc515: Support bigint branch and base selectors in subscription query keys without collisions or input mutation.
- 7451073: Avoid repeatedly cloning query operators and rediscovering immutable graph dependencies while checking hydrated query results for reuse.
- dcbf905: Allow bounded compression overhead for valid large sync messages while independently enforcing decoded-size and staged encoded-byte limits.
- eab8ef2: Bound latest-record storage lookups to one visible entry instead of collecting the entire matching history prefix, reducing repeated-update cost for rows with deep histories.
- 4d12226: Remove the pluralize-esm dependency. The intermediate bounded automatic naming convention is superseded in this release by explicit table-local relationships: declare forward navigation with `s.rel` and reverse navigation with `s.reverse`; no automatic reverse names or pluralization remain. When changing navigation names, update includes, reverse declarations, relation filters, and permission hops together. Preserve stored column definitions and every column-to-target mapping when converting existing references; an equivalent conversion preserves schema identity and stored data. See the explicit-relationships migration guide in this changelog.
- 26b2972: Release pending read ownership when native, WASM, or React Native consumers cancel or close. Preserve transaction read ordering and defer query coverage cleanup safely while storage work owns the node.
- b825808: Load explicit CLI env files before dispatch and remove their flags from subcommand arguments.
- 8df59fb: Reject missing or malformed values for CLI flags instead of falling back to unrelated arguments.
- af80ba3: Fold uniquely owned join-bucket changes at tick commit, avoiding accumulated history copies on subsequent updates while preserving shared snapshots.
- 60ca5ac: Avoid empty producer-transition allocations and redundant grouping of incremental join-index updates while preserving shared snapshots.
- 818ff95: Use wire protocol v3 for deployment-aware catalogue policy snapshots. Wire v2 peers now fail the handshake explicitly; upgrade clients, native runtimes, and Edge/Core servers together. Hosted clients require a compatible server deployment. Storage formats are unchanged and legacy stored-data upgrade paths remain supported.
- 8beadb3: Avoid redundant dependency-signature lookups while selecting evaluator memo and operator scopes.
- 8f0812e: Document and verify directly inferred relation APIs across includes, required includes, row types, forward/reverse hops, typed app declarations, slices, and separately authored schema/app/permissions modules. No generated relation catalogue is needed.
- b768f0c: Reject nested reference arrays during DSL schema construction.
- 3ab43d3: Treat empty prepared IN predicates as false before late parameter binding.
- 429f643: Reject enum defaults that do not match their declared variants during schema conversion.
- 141dcc9: BREAKING CHANGE: Reads, inserts, updates, and deletes now require explicit policies. When no permissions are defined or a table has no policies, the server rejects writes and returns no data for reads.

  This changes existing behavior: define policies for every table and operation your app needs before upgrading. Optimistic local writes remain possible, but the server will reject writes without a matching permission.

- 326e00f: Jazz now uses explicit table-local relationship declarations instead of inferred relationship names. `s.table(columns, relations)` now requires a relationship map (use `{}` when empty). Store references as `s.uuid()` or UUID arrays, declare forward navigation with `s.rel(targetTable, column)`, and declare reverse navigation with `s.reverse(sourceTable, forwardRelationName)`.

  Jazz no longer adds automatic reverse relationships or derives relationship names from reference-name suffixes and pluralization. Declarations are validated in TypeScript and at runtime, including cross-table targets, reserved names, and conflicting reference targets. Equivalent declarations preserve existing core reference metadata and storage identity. The examples, starters, permissions, and migration tooling now use the explicit API.

  ### Migrating an existing app

  Follow this order when upgrading from alpha.55 to alpha.56:

  Run these commands from the app package directory. The examples assume its schema is in `src`; replace `src` with the actual schema directory in every command. Repeat the checks for each independently defined app schema, keeping separate snapshots.
  1. **Before upgrading Jazz packages**, export the public compiled schema with the existing alpha.55 packages and schema. Keep this file outside the schema directory and preserve it through the upgrade:

     ```sh
     pnpm exec jazz-tools schema export --schema-dir src > schema-before-alpha56.json
     ```

  2. Upgrade the app's Jazz packages together to alpha.56, then convert the whole app to the schema form below. Preserve the underlying schema identity and update query and permission callers together.
  3. Export the converted schema with alpha.56:

     ```sh
     pnpm exec jazz-tools schema export --schema-dir src > schema-after-alpha56.json
     ```

     Run this comparison using Node. It removes only table-local navigation metadata (`relations`), which does not enter storage identity, and checks everything else. It preserves column order, defaults, reference targets, indexes, branch keys, and any other exported metadata; object property order does not matter.

     ```sh
     node --input-type=module <<'NODE'
     import { readFileSync } from "node:fs";
     import { deepStrictEqual } from "node:assert";

     function structure(file) {
       const schema = JSON.parse(readFileSync(file, "utf8"));
       for (const table of Object.values(schema)) delete table.relations;
       return schema;
     }

     deepStrictEqual(
       structure("schema-after-alpha56.json"),
       structure("schema-before-alpha56.json"),
       "Stored schema changed: inspect the conversion before deploying; do not reset data.",
     );
     console.log("Stored schema structure is unchanged.");
     NODE
     ```

     If this comparison fails, stop before deploying and inspect the conversion. Do not remove additional fields to make it pass or reset existing data. If you already upgraded without a snapshot, recover the original schema and alpha.55 package versions in a separate checkout and export there; do not reconstruct the baseline from the converted schema.

  4. Run the app's TypeScript checks (for example, `pnpm exec tsc --noEmit` where that is the app's check command) and `pnpm exec jazz-tools validate --schema-dir src`. Exercise the app's queries and permissions against its existing data before deploying. Republish permissions if you changed their authored traversals. Structural equality does not prove that every query and permission caller is correct.

  Every `s.table` now requires two arguments: stored columns and explicitly named relationships. Pass `{}` when there are no relationships. Replace `s.ref(target)` with `s.uuid()` and move the target information into a forward relationship declaration. **To keep the same schema identity and existing data, preserve every stored column name, modifier, default, optionality, array shape, and reference target.** Add a matching forward relationship for every former reference column, even if no query currently uses it. This conversion needs no data rewrite or database reset.

  Before:

  ```ts
  const schema = s.defineSchema({
    users: s.table({ name: s.string() }),
    posts: s.table({
      title: s.string(),
      authorId: s.ref("users"),
      editorId: s.ref("users").optional(),
      reviewerIds: s.array(s.ref("users")).default([]),
    }),
  });
  ```

  After, with the same schema identity:

  ```ts
  const schema = s.defineSchema({
    posts: s.table(
      {
        title: s.string(),
        authorId: s.uuid(),
        editorId: s.uuid().optional(),
        reviewerIds: s.array(s.uuid()).default([]),
      },
      {
        author: s.rel("users", "authorId"),
        editor: s.rel("users", "editorId"),
        reviewers: s.rel("users", "reviewerIds"),
      },
    ),
    users: s.table(
      { name: s.string() },
      {
        postsViaAuthor: s.reverse("posts", "author"),
        postsViaEditor: s.reverse("posts", "editor"),
        postsViaReviewers: s.reverse("posts", "reviewers"),
      },
    ),
  });
  ```

  The required `authorId`, optional `editorId`, and array `reviewerIds` keep their names and still point to `users`. The array also keeps its `[]` default.
  - Declare a forward relation for **every former reference column**, including references not currently used by an include.
  - Declare reverse relations explicitly if your queries or permissions use them. The second argument to `s.reverse` is the **forward relationship name**, not its UUID column. No reverse navigation is added automatically.
  - You can keep previous navigation names, as above, or choose names such as `authoredPosts`. For example, renaming `author` to `writer` keeps `authorId` unchanged, but requires `s.reverse("posts", "writer")` and `.include({ writer: true })` in place of the old names. Update `hopTo`, relation-based query filters, and permission `hopTo` calls too, including names used in untyped query objects.
  - Relationship names cannot shadow columns, `id`, reserved `$...` fields, or reserved prototype names. If an old include replaced a same-named UUID column, preserve that stored column and choose a distinct relationship name instead.
  - Keep `{}` on tables without relationships, including tables in test fixtures, migration schema witnesses, and dynamically generated schema source. Migration operations such as `s.add.ref(...)` and `s.drop.ref(...)` remain separate APIs; do not mechanically replace those operations.
  - Update schemas shared by browser, server, and React Native consumers. Complete the structural comparison, TypeScript, validation, and app checks above before deploying.

  ### Migrating with an agent

  Point your coding agent at this guide and the release notes, and give it this instruction:

  > Follow the before/after schema export commands and Node comparison in this guide, using the app's actual schema directory. Export with alpha.55 before upgrading, convert the whole app to alpha.56 while preserving stored schema metadata, and update query and permission callers to the declared relationships. Export again and compare, removing only each table's `relations` metadata. Stop before deployment if the comparison fails. Run the app's TypeScript checks, schema validation, and queries and permissions against existing data. Never reset data to make the upgrade pass.

  Have it use the actual app schema directory as described above and check every schema consumer (browser, server, React Native, tests, migration schema witnesses, and generated schema sources). Preserve all column definitions and reference targets, indexes, and branch keys. Update permission traversals together with the declared relationship names. Keep existing stores and pending writes.

  ### Schema identity and existing data

  The underlying mapping, for example `posts.authorId → users`, already participates in schema identity and is used by core query validation and indexing. Both the old `s.ref("users")` and the new `s.rel("users", "authorId")` produce that same mapping.

  An equivalent conversion that preserves column definitions and all reference targets requires no stored-data rewrite or database reset. Keep existing stores and pending writes; do not clear browser data as an upgrade shortcut. Relationship aliases and reverse navigation declarations do not themselves change stored schema identity: renaming `author` to `writer` only changes the authored query API, provided you also update reverse declarations and callers.

  Replacing a former reference with a plain UUID **without** its forward declaration removes reference metadata; changing its target changes that metadata. Those are actual schema changes, not the API-only conversion described here. Do not use a database reset as a migration shortcut or assume an empty migration can retarget references.

  The docs, maintained examples, starter templates, permission examples, and schema-export tooling have been updated to this API. Historical fixture producers pinned to older published packages intentionally retain their original API.

- d682de3: Reuse safe startup ancestry checks and compiled subscription programs across repeated opens.
- dcc8786: Preserve first-result consumer lifetime through shared query hydration, avoiding retained indexes for disposable reads without disturbing live subscriptions.
- b1768f9: Shut down native servers gracefully on SIGINT and release their storage directories for immediate reuse.
- c24cf42: Preserve transport backpressure and failure reporting, and drain pre-handoff messages before requesting fresh authority snapshots so fast confirmations can settle subscriptions safely.
- c82bca7: Maintain ordered ArgMin/ArgMax candidates incrementally instead of rebuilding whole groups on each update, reducing repeated-update costs for deep row histories while preserving deterministic ties and retractions.
- c85fc85: Fix standalone Inspector admin queries returning empty rows for policy-protected tables, including connections routed through Edge to Core.
- 4dae482: Preserve exact BigInt values and explicit null mutations in Inspector while keeping untouched nullable Boolean inserts null.
- Preserve Inspector BYTEA equality filters when restoring queries from a URL by retaining their exact byte values.
- Release Inspector clients after failed setup or unmount, including clients that resolve after setup ends, while preserving ephemeral admin credentials.
- 4b4dccc: Keep Inspector connections usable when persistence fails while removing legacy stored credentials if migration cannot rewrite them.
- 3ab44b2: Build Linux native Node packages on an AL2023-compatible userspace and publish Linux ARM64 GNU bindings alongside x64. Keep native compiler provenance and build caches tied to the selected Linux baseline.
- 8754346: Publish ready local subscription results after storage wakes the query runtime, even when another query has already completed its work. This prevents a cold subscription from remaining loading until an unrelated write.
- 9f036c6: Resolve permission inheritance through declared forward and reverse relationship names. `allowedTo.read/insert/update/delete` now share the relationship namespace used by `hopTo`; `*Referencing` takes a declared forward name on its source table. Raw FK column names and inferred aliases are rejected. Reject unsupported reverse `maxDepth` options instead of silently ignoring them, while preserving existing native authorization and raw policy IR semantics.

  Permission `hopTo` resolves the exact declared name, including leading and trailing whitespace, consistently with query navigation and `allowedTo`.

- 837dd45: Fix live queries using inherited permissions on optional references so successful writes sync and denied writes report authorization errors instead of a terminal layout error.
- 5c76456: Multiplex bounded, prioritized channels over each connection so large uploads and deliveries do not block independent small queries. Compression now retains history per ordered stream, while explicit message dependencies preserve catalogue, session and transaction ordering across streams.

  Run immutable chunk requests and responses independently of suspended database operations, fixing large-value reads that could wait indefinitely (#3164). Retained partial messages have enforced deadlines and bounded memory; a stalled connection cannot prevent other connections from progressing.

  Preserve transaction settlement when an older query delivery is replaced while waiting for missing row bodies. Discarded deliveries retain transaction identity without publishing obsolete rows, and later partial deliveries cannot regress an already-settled transaction.

  This requires the wire v3 client/server update together. It does not change storage encoding.

- 2b81ec5: Yield database owner ticks while a retained operation or external read holds the node lock, and wake the owner when an external read releases it. This prevents stalled owner queues without busy-loop scheduling or losing deferred cleanup.
- a1087ff: Preserve nullable and defaulted payload-enum fields in inferred initialisation values.
- ff6c4b0: Reject unsupported payload-enum query literals and bindings with structured query errors.
- f6af418: Fix permission-scoped inserts and updates of JSON larger than 64 KiB. Policy candidates preserve JSON scalar types without loading large payloads that ownership checks do not inspect.
- ebe9ffd: Borrow compiled evaluator signatures and reuse prepared memo lookup inputs within a node evaluation, preserving live readiness checks.
- 062d00f: Maintain shared join indexes at their producer and consume read-only transitions, avoiding full-bucket publication snapshots and unchanged-row replay in semi/anti-joins.
- 87e87eb: Reject raw storage writes whose supplied key differs from the record primary key before publishing physical or logical changes.
- b2d1588: Recover pending local writes without blocking startup on incomplete or deeply nested causal history.
- bc2e05b: Reject duplicate legacy table declarations during schema validation instead of silently replacing one declaration.
- cca9fa6: Reject user-defined schema columns that use the reserved magic-column namespace.
- b5c1168: Allow ordinary mutations on rows made visible again by a restore. A retained deletion-register history no longer causes a restored row to be rejected as already deleted; rows that remain deleted still reject ordinary writes. This includes deleting a row again after either a standalone or transactional restore, without changing branch-view rules or the stored history format.
- 7f23ca3: Preserve authored schema and physical table identities when validating reads across migrations. Unchanged exclusive reads can commit after a table or column rename, while concurrent matching inserts and ambiguous reused table names still reject.
- 8465b97: Explain schema catalogue HTTP 404 failures with guidance to check the server URL and app ID, and to wait for regional readiness before retrying a newly created Cloud app. Preserve the original HTTP status and single-request behavior; no automatic retries are added.
- 400f09e: Reject reserved schema control names and collisions during generated schema construction.
- 2ce7b6f: Use selective live query hydration for current reads and subscriptions, avoiding full-table work for supported indexed permissioned queries. Preserve validated enum parameter types when retained subscriptions and one-shot reads share a binding source.
- 641e0d9: Require separate schema and permissions inputs for `startLocalJazzServer` and `createJazzSession` from `jazz-tools/backend`. Explicit permissions replace all embedded policies, including when the bundle is empty or omits tables. A local server can still start without a schema for a later `deploy`.

  Remove the `mergePermissionsIntoWasmSchema` export from `jazz-tools/testing`. Pass the schema and permissions separately instead.

- 9261ef7: Scope WebSocket connection limits and admission cleanup to each server instance, preventing independent servers in one process from evicting each other's clients.
- cb3f156: Auto-attach the development inspector when using `JazzSessionProvider`, with an `autoAttachDevTools` opt-out.
- 1319778: Configure matching JWT issuer and audience in Better Auth and hybrid starters so valid provider sessions can log in to Jazz.

  Enable the embedded inspector in Next development through a loopback asset server and a rewrite that preserves application routes. Honor inspector opt-out and leave production configurations unchanged.

- 868947d: Reduce subscription opening work by compiling only the executing local subscription program, preserving its authorization checks.
- b21f0e6: Preserve structural column defaults in generated migration witnesses, including UUID-to-relation migrations. Validate each witness against its own canonical schema, while preserving ordinary default-only metadata changes without row transforms.

  Correct CLI-computed schema hashes to match existing Rust/server identities for defaults, merge strategies, and branch bindings. Stored schema identities and wire formats do not change.

- 41ba644: Drain transaction preparation and outstanding reads before committing, retain deferred failures for later waits, and prevent ordinary mutations from overtaking preparation. Preserve synchronous row results and reject deferred admission after rollback, shutdown, discard, or account changes.
- 99630e1: BREAKING CHANGE: `deploy` is now the single operation for publishing schemas, permissions, and migrations, both through the CLI and programmatically. Deployment now requires explicit permissions and fails before publication when a required migration is missing. Empty permissions remain valid and deny all access. First deployments, unchanged schemas, and compatible transitions do not require a reviewed migration file. The `noVerify` / `--no-verify` bypass is no longer supported.

  Removed the public `pushSchema`, `pushPermissions`, and `pushMigration` exports, the package-root `publishStoredSchema` and `publishStoredPermissions` exports, and the `migrations push` CLI command.

- 7577382: Reject duplicate scalar enum variant names at every schema ingress boundary while preserving distinct case-sensitive names and declaration order.
- a0bf500: Release unused subscriptions after their original cleanup deadline even when updates continue arriving, while preserving subscriptions with active listeners.
- 2c20990: Generate executable identity migrations when existing UUID and UUID-array columns gain explicit relations, preserving stored values and including referenced tables in migration witnesses. Continue rejecting reference removal, retargeting, and unsupported simultaneous column changes.
- 9cb4c40: Reject schema references to undeclared tables and references on non-UUID columns with source-specific diagnostics. References to declared tables may still point to rows that do not exist yet.
- Updated dependencies [9462d65]
  - jazz-rn@2.0.0-alpha.56
  - jazz-wasm@2.0.0-alpha.56

## 2.0.0-alpha.55

### Patch Changes

- 194df87: Fix live synchronization of deleted rows protected by account-based ownership permissions, so other clients remove the deleted row without requiring a reload.
- 5245e76: Preserve the source ordering and duplicate references when hydrating forward array foreign keys, instead of deduplicating and sorting the referenced rows by ID. Support reverse UUID-array correlations in array subqueries.

  [PR #2865](https://github.com/garden-co/jazz/pull/2865).

- c76144e: Persist declared-index updates with their base publication even while a subscription waits for missing content.
- 3c228b4: Avoid copying the complete previous supporting-fact set when receiving a subscription snapshot; only changed peer facts are cloned into the local delta.
- e897992: Build complete subscription supporting-row manifests from borrowed maintained facts without constructing an intermediate owned fact set.
- bf942bf: Reduce validation overhead when reading inline scalar values without changing their storage encoding.
- 9c6c465: Propagate browser worker auth-update failures to readiness and recover them on an explicit reconnect.
- 72fad2f: Avoid full-table reads when staging browser transaction updates, and prevent failed write-merge reads from leaving patches staged for a later commit.
- b082ff8: Reclaim obsolete IndexedDB pages during writes under exclusive ownership, and safely retire browser runtime storage handles during schema changes and reconnects. Existing unused pages are not automatically collected.
- 9d69a5b: Fix browser startup freezing when restoring pending uploads requires a cold IndexedDB read. Recovery now yields to asynchronous storage instead of blocking its callbacks.
- 69e26c2: Reduce browser storage lookup overhead by copying only the requested row value instead of unrelated values in the same page.
- dd491a8: Avoid opening a competing browser worker generation when runtime startup is slow; preserve a bounded initialization deadline and explicit worker handoff.
- f8ace50: Reject intentional WebSocket closes before server hello.
- 2cdb74e: Reject browser reconnect readiness waiters after terminal worker failure.
- 06314ba: Keep native transport credentials mode-exclusive across live auth transitions.
- fa458a8: Serialize native transport retirement before admitting replacements.
- 6fcf012: Reduce CPU work and allocations when validating nested records during local query delivery, without changing storage encoding or accepted values.
- 0ad46d9: Keep catalogue publications equivalent across table declaration order, so persistent clients can reopen after multi-table migrations against a running authority.
- 5245e76: Move subscription result buffering into the Rust core and remove redundant TypeScript query preparation and row caches. Preserve scoped relation reads and wait for complete recursive query results before publishing them. Direct binding consumers must adapt to removal of the Rust–TypeScript `prepareQuery` API.

  [PR #2717](https://github.com/garden-co/jazz/pull/2717).

- 80fe6b1: Add regression coverage for allowed and denied inserts governed by correlated relationship policies, including server validation of optimistic writes.
- 3833c7c: Handle comparisons with null correctly inside correlated relationship policies, and cover allowed and denied deletes under those policies.
- 178a4b8: Add regression coverage for correlated relationship policies on updates and reverse-relationship reads, including allowed and denied operations.
- cc77ea3: Automatically rebuild declared indexes once when opening older local databases, repairing missing or stale entries while preserving primary records and pending unsynced writes.
- a73288a: Stop deferred subscription work from rearming server ticks after activity has ended.
- 05eea99: Fix browser subscriptions remaining stale after a transaction commits with deferred local persistence.
- fe80d90: Reduce the work needed to recover deletion and restore witnesses when delivering locally cached rows to a subscriber, using the existing current-row indexes directly.
- 5245e76: Register mutation waits with the Rust runtime before the upstream connection is ready, so writes made during connection startup can still settle correctly.

  [Implementation](https://github.com/garden-co/jazz/commit/d084613778f01f213490a4b6f1826d8ad0cad118).

- 5b64a9d: Avoid scanning every sibling version in a large transaction when validating an existing exact parent row. Preserve branch, layer, identity and incomplete-parent checks.
- bf79ef4: Keep averages of large finite numbers finite when their intermediate sum overflows, while preserving cancellation, subscription updates and retractions.
- b8a8d0d: Fix updates that could fail to settle after concurrent local writes by stopping redundant merges once only one underlying edit remains.
- 82dd0e6: Fix migration stub generation for bare UUID columns, including nullable UUIDs in newly added tables.
- 42eb629: Keep overlapping subscription hydration pending until earlier shared-node snapshots are installed, avoiding internal subscription failures while those dependencies resolve.
- 5245e76: Copy immutable version-row wire bytes in bulk instead of rebuilding their fields during synchronization, preserving the existing encoded values.

  [PR #2923](https://github.com/garden-co/jazz/pull/2923).

- f93c6e0: Reduce subscription publication work for small changes by tracking changed source facts across drains and retries.
- 5245e76: Keep standalone Node, browser, and React Native subscriptions loading until the first complete local result is ready, avoiding a premature empty snapshot when matching rows are already stored. Browser and React Native foreground databases wait for their persistent worker or relay to supply the initial inputs. Genuinely empty results still finish loading without waiting for server sync, and initialized subscriptions do not return to loading on disconnect. Preserve joined-row identities when resetting subscription inputs.

  [PR #2986](https://github.com/garden-co/jazz/pull/2986).

- b3d67b8: Keep Inspector admin secrets out of persisted connection state, generated configuration, and URL fragments.
- 0afb098: Keep other clients responsive when a loopback WebSocket client is backpressured, while preserving message delivery order.
- 5245e76: Reduce work for maintained query updates by deriving unbounded-window membership from changed records and constructing root-position maps only for outputs that use them. Reuse proven equivalent witness payloads across maintained roles while keeping incompatible executions isolated.

  [PR #2921](https://github.com/garden-co/jazz/pull/2921), [PR #2922](https://github.com/garden-co/jazz/pull/2922), [Witness reuse implementation](https://github.com/garden-co/jazz/commit/611b30c4a8b9d0788a0d39d514b8002da529c48a).

- a54c7e6: Preserve authored transaction identity when a persistent owner relays reads across schema migrations. Old and new schema reads retain original row bytes and authored metadata, and renamed-table witnesses retain their exact branch.
- 020bd39: Support nested correlated relationship checks inside permission policies. Private dependency rows can authorize an operation without becoming readable to the caller.
- d064481: Fix deployment of policies with correlated `exists` clauses nested in boolean branches.
- 22d04ef: Allow persistent clients that missed a published migration while offline to reopen and fetch the new schema catalogue without an application-managed old-schema bootstrap. Preserve cached rows and pending edits, and keep unpublished schemas unavailable.
- 5825187: Recover permissions publication when a schema migration is already durable but missing from the running server. Reject missing or ambiguous lineage before advancing the permissions head, so publication can be retried safely.
- 5f5d279: Improve first sync and sequential writes by maintaining physical supporting rows directly and sending an initial snapshot followed by changes to that set.

  **Sync protocol upgrade:** this release uses wire protocol v2 and cannot sync with v1 peers. Upgrade clients, native runtimes, and self-hosted Edge/Core servers together; hosted clients need a compatible server deployment.

  Subscription membership and resume cursors are now kept in memory. After a process restart, remote reads obtain a fresh supporting snapshot; local-first reads can still use eligible persisted data. Existing subscription caches and cursors are discarded automatically on upgrade, while application rows, transaction history, pending local writes, and the catalogue are preserved. No manual local-storage reset is required.

  Keep pending reads progressing across in-process catalogue rebuilds and cache eviction without reusing invalidated subscription answers. Local-first foreground reads continue to accept a fresh answer from their local owner while offline.

  Fix native foreground reads that could stall when a refreshed subscription needed to fetch a missing row version. Host-admitted native relays now install their session scope and request the matching authenticated upstream link, preserving isolation between sessions and support for reopening existing account databases.

  Bind native permission advice to the admitted foreground identity and claims. Claims changes cancel outdated advice, and server receipt revisions are no longer confused with local claims counters.

- 64a449b: Avoid synchronous transaction preflight reads that can block the host while queued database work holds the owner lock.
- e4460da: Reject null defaults for required columns during schema validation, before publishing the schema.
- 5245e76: Require all non-nullable insert fields, including array fields that previously defaulted to `[]`. Report upsert input validation failures through `onMutationError` and the write handle’s `wait()`; transaction failures are available through `commit().wait()`. Missing upsert fields are validated asynchronously because they may be valid for an update but invalid for a new row.

  [PR #2717](https://github.com/garden-co/jazz/pull/2717).

- b5692a6: Preserve nullable parent correlation keys when required nested includes filter rows, avoiding collector descriptor mismatches without changing required-match eligibility.
- 5245e76: Reject explicit table-level `id` columns in schema definitions, including at TypeScript type-check time. Every table uses its automatically generated UUID `id` as its row identity; remove authored `id` declarations before upgrading.

  [PR #2873](https://github.com/garden-co/jazz/pull/2873).

- b6eda5a: Reuse the complete supporting-row set during subscription publication bookkeeping instead of reconstructing it again.
- 5ebd45f: Reuse vacant server session slots while rejecting stale handles from previous identities.
- 409289a: Deliver local React Native writes to foreground subscriptions immediately and refresh policy-dependent subscriptions when membership data changes.
- 681f5c9: Rebuild catalogue runtimes safely while query hydration or auxiliary chunk reads are pending, and defer catalogue ingress until in-flight publications settle.
- 3026901: Keep replacement subscriptions live across catalogue rebuilds by scoping Groove subscription handles to their creating runtime.
- 2359534: Avoid repeatedly reading transaction status for writes already acknowledged locally while waiting for server confirmation, improving offline bulk-write performance.
- 13e96f8: Compare incoming supporting snapshots as sorted vectors without building temporary fact trees.
- a26a596: Reduce cold-load and large-transaction overhead by avoiding repeated ancestry scans, record conversions, and unused query payloads. Preserve complete subscription results when recursive evaluation or data hydration resumes across runtime turns.

  Check subscription completeness against its typed schema view, including views registered on an initially empty owner.

  Keep provisional enum registries coherent when adopting the authority catalogue, preserving offline enum values and preventing provenance failures on subsequent reopen.

- 678c2ed: Reduce transaction-status polling cost by reading fate and durability fields without decoding unrelated transaction payloads. Preserve pending-persistence and rejection checks.
- 5245e76: Support permission policies with `EXISTS` checks that do not reference the outer row, such as checking whether the current user has an admin grant. Empty grant sets deny access, multiple matching grants do not duplicate results, and removing the last grant revokes existing subscriptions.

  [PR #2880](https://github.com/garden-co/jazz/pull/2880).

- Updated dependencies [5245e76]
- Updated dependencies [060083d]
  - jazz-wasm@2.0.0-alpha.55
  - jazz-rn@2.0.0-alpha.55

## 2.0.0-alpha.54

### Patch Changes

- bc70549: Jazz now runs on Groove, a new low-level database engine built around incremental view maintenance. It shares work between similar query subscriptions and provides a foundation for improving correctness and performance. This release remains an alpha: please report anything that breaks or feels slow.

  ### Breaking storage change

  Alpha.54 changes the storage format without automatic migration from alpha.53. If you have existing production data, contact us for migration help before upgrading. For a fresh start, create a new Jazz Cloud app, or use the alpha.54 CLI with fresh server storage when self-hosting. Future storage-format changes will include automatic migration.

  ### Four major changes

  **Simpler read tiers.** Choose `local-first` for immediate local results with background sync; `remote-if-possible` for remotely confirmed state when online, local fallback when offline, and immediate local writes; or `remote` for remotely confirmed state only, including confirmation of local writes before they appear.

  **Clearer account lifecycle.** Configure your account and client once with `createJazzSession`. The session manages auth transitions, graceful shutdown and client replacement. Framework adapters expose that state without app-owned lifecycle plumbing. External authentication can use `loginOrRegisterJWT`; explicit linking remains available for attaching a fresh external identity to an existing account. Backends use the same session API with `initial: { backendSecret }` or `becomeBackend({ backendSecret })`.

  **Build-your-own branching.** Define branched table views with `branchBy` columns and explicit head/base view options. Branch identifiers can be strings or references to your own branch table, letting your app define branch metadata, workflows and row-level permissions.

  **Large values are ordinary columns.** Store files and streams in `s.bytes()`, large JSON documents in `s.json()`, and text in `s.string()`. Read complete values or stream supported ranges and JSON subpaths; update them with byte patches, appends, JSON edits or string splices. Content-based chunking in a Prolly Tree makes these operations efficient while retaining ordinary column permissions and query semantics. JSON and rich-text merge strategies are planned for a later release.

  ### API migration checklist
  - Replace `Db.subscribeAll` with `Db.subscribe` for complete current results.
  - React and React Native `useAll` now return `{ data, isLoading, error }`; `useAllSuspense` still returns rows.
  - Replace `localUpdates` and `propagation` options with read-tier selection.
  - Move account/client lifecycle handling to `createJazzSession` and the framework adapters. Linking happens outside contexts; the session gracefully shuts down the old client and syncs pending writes before replacing it.
  - `$createdBy` and `$updatedBy` are non-null `{ account, identity: { issuer, subject } }` values. Use `.account` for ownership comparisons. SYSTEM authorship uses a reserved account and issuer, with the originating node as subject.
  - Direct `jazz-wasm` consumers must await `acceptSubscriber` and `acceptSubscriberWithSelfSignedProof`.

  ### Reliability improvements

  This release also improves browser and React Native persistence, offline recovery and reconnect behavior; query correctness across relations, aggregates, branches and permissions; and authentication and framework lifecycles. It includes fixes for large JSON relation values, native runtime packaging and generated starter apps.

- Updated dependencies [bc70549]
  - jazz-wasm@2.0.0-alpha.54
  - jazz-rn@2.0.0-alpha.54

## 2.0.0-alpha.53

### Major Changes

- 891b264: BREAKING CHANGE: make `jazz-tools/dev` and `jazz-tools/testing` catalogue deploy helpers accept schema, permissions, and migrations JavaScript objects directly instead of project file paths.

### Patch Changes

- f072cb0: feat: the localStorage key used by React's `useLocalFirstAuth` helper is now configurable, to support multiple Jazz apps on the same origin
- 397f84d: Add `Db.disconnect()` and `Db.reconnect()` for temporarily pausing and resuming sync without shutting down local storage.
- 9bee23f: The postMessage "devtools protocol" bridge and the separate browser-extension/devtools build are deleted now that the inspector overlay talks to the data layer through its own worker connection: `attachDevTools`, `createExtensionJazzClient`, `createEmbeddedJazzClient`, and the `DevToolsAttachment` type are no longer exported.
- 27f9ced: Add an in-app inspector overlay to the Jazz dev plugins. During development the Vite, SvelteKit, and Next plugins now mount a floating toggle (or press `Alt+Shift+J`) that opens the embedded Jazz inspector docked at the bottom of your app — no separate window or setup. It's on by default whenever the dev plugin is in use and is dropped entirely from production builds. `jazz-inspector` now ships as a dependency of `jazz-tools`, so it no longer needs to be installed separately.
- 2d5f287: The embedded inspector overlay now connects to the data layer through its **own worker connection** instead of the postMessage "devtools protocol" bridge. The overlay reads the host app's connection config from a same-origin `window.__jazzInspectorHost` handle and **joins the host's local store** — it reuses the host's IndexedDB namespace, broker SharedWorker URL, and identity, so it sees the host's actual local data (including unsynced local-only rows) and works offline; no `serverUrl` is required. It receives the host's active subscriptions via a one-way push for Live Query.
- bc74a87: Avoid firing a duplicate immediate tick for auto-committed direct writes. Insert, update, upsert, delete, and restore ticked both inside `commit_batch` and unconditionally afterwards; the follow-up tick now only fires when the write does not auto-seal, halving scheduler pressure during bursts of bare writes.
- 42e77fd: Allow `createDb({ driver: { type: "memory" } })` to create a standalone in-memory database without connecting to a sync server.
- Updated dependencies [bc74a87]
  - jazz-rn@2.0.0-alpha.53
  - jazz-wasm@2.0.0-alpha.53

## 2.0.0-alpha.52

### Patch Changes

- 6284805: The browser broker SharedWorker is now shipped as self-contained, bundled ESM. It was previously unbundled with bare `../runtime/*.js` imports, and because its `new SharedWorker(...)` call is indirected past bundler worker-detection, Turbopack, webpack and Vite copied it verbatim and its imports 404'd on load — crashing every Jazz app under `next dev` / `next build` and `vite build` with "Browser broker SharedWorker failed to start". (`vite dev` masked it.) Fixing it in the package build covers all frameworks.
  </content>
- 97751b0: Make `db.one(...)` execute with a root query limit of one instead of fetching every matching row and discarding all but the first.
- a9c0cf4: Fix `deleteClientStorage()` hanging forever when called on a persistent browser Db before any table or query has been used.
- 57d2eb6: Add a `merge("g-set")` strategy for non-nullable array columns. Concurrent writes converge to the grow-only union of every replica's elements, deduplicated and sorted into a canonical, byte-identical order, so an element written by one replica is never dropped by a concurrent write that never saw it.
- fc12a85: Removed `TestingServer` and `pushSchemaCatalogue` from `jazz-tools/testing` exports and consolidated `startLocalJazzServer` and `deploy` as the canonical way of starting a Jazz sync server and publishing schema changes.
- a454f75: Add a `jazz.server.active_websockets` OpenTelemetry gauge reporting the server's current inbound WebSocket connection count. It is exported over OTLP only when the crate is built with the `otel` feature and `OTEL_EXPORTER_OTLP_ENDPOINT` is set; builds without the feature are unaffected.
- 03dcdbb: Add a `jazz-tools/shared` entry point exposing the framework-agnostic utilities the React, Svelte and Vue bindings use to turn a live query into a reactive, in-place-updated result set: `applyDelta`, `reconcileArray`, `RowChangeKind`, and the supporting types (`SubscriptionDelta`, `RowDelta`, `QueryBuilder`, `QueryOptions`, and the orchestrator's `SubscriptionsOrchestrator` / `CacheEntryHandle` / `UseAllState` shapes). The in-repo bindings now consume this shared surface so an external author can build their own binding (e.g. a signals-based `useAllSignal`) on the same utilities.

  This is an advanced, use-at-your-own-risk surface — the internals our framework bindings are built on, surfaced for reuse. It is not covered by semver; the orchestrator/cache-entry/delta shapes may change between releases, so pin a version if you depend on it.

- 6c17100: Route persistent browser runtimes through a SharedWorker broker so tabs for the same Jazz app share one IndexedDB-backed leader runtime instead of each opening independent storage handles. The broker coordinates leader promotion, follower message ports, schema compatibility, visibility hints, storage resets, and failover after tab or worker crashes, preserving pending local writes while the durable path reconnects.

  **Breaking change — browser support:** persistent browser mode now requires `SharedWorker`, `MessageChannel`, and Web Locks support. Browsers or embedded webviews missing those capabilities will reject `createDb()`/`createJazzClient()` startup for persistent storage instead of using the previous BroadcastChannel tab-election path. Use a supported browser runtime for persistent local storage, or switch to the memory driver with a `serverUrl` in unsupported environments.

- f958471: Add first-class Solid support in `jazz-tools`, including `createSolidJazzClient`, `JazzProvider`, `useAll`, and `useLocalFirstAuth`, plus Solid-specific build/test configuration and lifecycle/auth/query coverage.
- Updated dependencies [ec543e3]
- Updated dependencies [6c17100]
  - jazz-wasm@2.0.0-alpha.52
  - jazz-rn@2.0.0-alpha.52

## 2.0.0-alpha.51

### Minor Changes

- 4a10478: Expose programmatic catalogue publication helpers from `jazz-tools/dev`.

  `jazz-tools/dev` now exports `pushSchema`, `pushPermissions`, `pushMigration`, and `deploy` so tools can publish schema, permissions, migrations, and full deployments without going through the CLI. The existing `pushSchemaCatalogue` compatibility path remains available.

### Patch Changes

- 4831e63: Avoid cloning the entire per-object sent-batch set when syncing. The forwarding walk and the server and client queue paths cloned the whole `sent_batch_ids` set just to test membership of a single batch. Since that set grows with a row's history, forwarding a frequently-updated ("hot") row did work proportional to its accumulated history on every update. Membership is now checked by borrow, so forwarding is independent of how much history has already been sent.
- 1670073: Fix a `batched_tick` deadlock that left React Native apps spinning at 100% CPU when a server-side query subscription was deferred waiting for its catalogue/schema.
  - `jazz-tools`: `RuntimeCore::batched_tick` now drains parked sync messages before deciding whether to reschedule, so a `CatalogueEntryUpdated` that arrives while a subscription is parked actually unblocks it instead of sitting in the parked queue forever. Progressless ticks no longer re-arm the scheduler, breaking the reschedule hot loop.
  - `jazz-rn`: `request_batched_tick` is now deferred off the JS thread (mirroring `schedule_mutation_error_delivery` and `NapiScheduler`) so the JS callback can't synchronously re-enter `batched_tick` and starve `setInterval` / rendering.

- b11d29b: Reject an oversized LZ4 frame on the pre-authentication WebSocket handshake before decompressing it, closing an unauthenticated decompression-bomb that could exhaust server memory. The inbound WebSocket message-size limit is also pinned explicitly.
- 7c8b790: Deduplicate Jazz clients per config so a single page runs one runtime per identity. Previously the Svelte and Vue bindings created an independent runtime for every `createJazzClient` call, so mounting several components for the same identity in one page produced coexisting runtimes in the shared WASM heap; abruptly tearing one down during active sync could corrupt the others' heap and surface as a `memory access out of bounds` trap. Client lifecycle now goes through a shared, refcounted, `Map`-keyed registry used by the React, Svelte, and Vue bindings (replacing the React binding's single-slot cache, which could not hold two distinct configs at once). Clients with the same config share one runtime; distinct identities (e.g. two principals on one screen) keep their own.
- 6c97d33: Fix a permissions race where a permissions head arriving before its bundle would flip the client into Enforcing mode without an authorization schema, causing local writes against every table to fail with `policy denied` until the bundle arrived. The mode now only flips when the bundle is applied.
- baa6494: Support `in` filters for id, enum, string, and reference columns, including empty `in` lists returning no rows.
- 7568ce7: Export `RowRefValue` and `SessionRefValue` types so user-defined helpers inside `definePermissions(...)` callbacks can be typed against row and session references.
- 76693c8: Export `resolveRequestSession` from `jazz-tools/backend` so RPC handlers can derive Jazz sessions from request bearer JWTs without manual decoding.
- 7d5ab71: Fix a crash when syncing a row with a long edit history. Forwarding a row's parent batches to a server walked the history recursively, so a row with a deep history chain (a few hundred edits is enough on a browser worker's stack) could overflow the stack — surfacing as "memory access out of bounds" on the client and out-of-memory on the server. The walk is now iterative and visits each parent batch at most once.
- 8421937: Fix update permission policies that compare `id` inside `EXISTS` checks.
- 88168eb: Add `includeDeleted()` to TS query builders for reading soft-deleted rows.
- 8cf401c: The inspector link logged on server startup no longer embeds the admin secret in the URL. If an admin secret is configured, a follow-up log line prompts you to enter it manually in the inspector.
- aea994f: The server now logs a prominent warning when local-first auth is silently auto-enabled because `NODE_ENV` is not set to `"production"`. Deployments that forget to set `NODE_ENV=production` will see the warning rather than running wide open with no indication.
- feea722: `withJazz` no longer adds `jazz-tools` to Next.js `serverExternalPackages`. Externalising it caused the SSR worker and the user's "use client" components to load separate React instances, so the SSR dispatcher was missing on jazz-tools' copy and `useSyncExternalStore` failed with `Cannot read properties of null` when prerendering pages like `/_not-found`. `jazz-napi` stays external (native binary); jazz-tools is now bundled.
- 6f7a83f: Fix an owner `db.update` of a backend-created row hard-deleting the row instead of updating it on persistent-storage clients. A client write can no longer downgrade a batch the server has already accepted, so the row survives and the update applies.
- daec2e2: `getSession()` (Svelte) and `useSession()` (Vue) now return reactive handles that track auth changes without destroying the provider.
  - **Svelte**: `getSession()` returns `{ current: Session | null }` — read `.current` in templates, `$derived`, or `$effect` and it updates automatically on login/logout.
  - **Vue**: `useSession()` returns `ComputedRef<Session | null>` — bind `.value` in templates or computed properties and it stays in sync via `triggerRef`.

- 56f7539: Bound the reconnect-storm amplification on the server's WebSocket path: enforce a per-`client_id` connection cap (4) with evict-oldest semantics so a single client_id cannot pin unbounded fan-out memory, and time out pre-handshake sockets after 10s so an unauthenticated peer cannot park resources without sending the `AuthHandshake` frame. Evicted clients receive a `RateLimited` error frame followed by a policy close.
- d0b2440: Rename the public Db callback scope types from `DbTransactionScope` / `DbBatchScope` to `TransactionScope` / `BatchScope`.
- 5c76bfc: Add soft-deleted row restoration with `db.restore(...)`.
- 7ab5830: jazz-tools server now has built-in support for OTEL
- 791e5e2: Stop the inert "memory access out of bounds" WASM trap from surfacing as an uncaught error when a page is reloaded or closed while two or more Jazz clients share the tab. Each client's WebSocket transport is abandoned mid-navigation and the dying page's WASM heap traps; the runtime now swallows that one specific trap inside the `pagehide` teardown window (on both the main thread and the worker), so it no longer reaches the console or the app's error handlers. A genuine out-of-bounds error during normal operation still surfaces.
- 5c76bfc: `db.update(...)` now fails when trying to update deleted rows, similarly to insert and delete.
- 03a71b2: Fix a cluster of `useAll` / `QuerySubscription` correctness and parity bugs across the React, Svelte and Vue bindings and the shared subscriptions orchestrator.
  - **Orchestrator:** New render-safe `computeKey()` / `peekState()` reads back the React rewrite. Subscription-setup failures no longer surface as unhandled promise rejections for callback-only consumers. On a session change, a settled entry is reset to `pending` and its listeners are told to clear, so a previous session's rows are dropped on logout/login rather than served until the new subscription's first delta.
  - **React:** `useAll` is rewritten on `useSyncExternalStore` with a `getServerSnapshot` path. Reads are render-safe (no subscription opened during render), an inline `app.todos.where(...)` no longer does render-phase work, SSR reads a seeded snapshot synchronously without a layout-effect warning, and a pending suspense query now suspends on its real entry promise (opened during render) instead of one a suspended effect could never resolve, while a not-yet-supplied query still suspends so the boundary shows its fallback. The JWT refresh is now deduped at the client level, so a second provider or a remount cannot double-fire it, and the refresh latch times out so a hung `onJWTExpired` can no longer wedge auth and silently drop every later expiry.
  - **Svelte:** `QuerySubscription` returns its unsubscribe directly from the effect (no shared mutable field to drop), drops `onDestroy` so it works inside `$effect.root` / `.svelte.ts`, always starts `current` as `undefined`.
  - **Vue (BREAKING):** `useAll` now returns `{ data, error, loading }` refs instead of a bare `Ref<T[] | undefined>`, so a failed query is distinguishable from loading or empty. Adds a Suspense-compatible `useAllSuspense`. Migrate `const todos = useAll(...)` to `const { data: todos } = useAll(...)`.
  - **Docs:** the `subscribeAll` JSDoc example uses `change.item` and the now-exported `RowChangeKind.Added` (the old `change.row` / `change.kind === 0` yielded `undefined`), and `SubscriptionDelta.all`'s freshly-allocated-per-delta contract is documented with a pointer to `applyDelta` / `reconcileArray`.

- Updated dependencies [1670073]
- Updated dependencies [6f7a83f]
- Updated dependencies [5c76bfc]
- Updated dependencies [791e5e2]
- Updated dependencies [5c76bfc]
  - jazz-rn@2.0.0-alpha.51
  - jazz-wasm@2.0.0-alpha.51

## 2.0.0-alpha.50

### Patch Changes

- 4fe985d: Fix the `jazz-tools` CLI silently exiting 0 without running any command when `dist/cli.js` is invoked through a pnpm symlink. `isMainModule()` now compares the realpaths of both `process.argv[1]` and `import.meta.url`, so the symlinked package path resolves and the CLI dispatches as expected.
- f463ae9: Drop Node.js 20 support. Minimum is now Node.js 22.12 (Jod LTS). `engines.node` is set to `>=22.12` on `jazz-tools` and `create-jazz`; consumers on Node 20 will see an `EBADENGINE` warning (npm/pnpm) or a hard install failure (Yarn).
- e00b73c: `Query.hopTo("relation")` now infers the destination table's row type instead of carrying the source table's type through. Consumers extracting the row type via `s.RowOf<>` (or any other consumer of the query builder's row type) now see the destination shape.
- c599e10: Fix the MCP server crashing with `ERR_UNKNOWN_BUILTIN_MODULE` on Node.js < 22 (and non-Node runtimes). The text-search fallback used when `node:sqlite` is unavailable no longer transitively imports `node:sqlite`: the pure MDX parsing helpers now live in a sqlite-free module, so the fallback loads and serves docs instead of failing. The fallback also emits a loud deprecation warning, since Node.js < 22 is no longer supported.
- e49cf4c: Moves write error propagation fully into the Rust runtime
- 5c0ef8f: Export `tracing` events as OpenTelemetry logs over OTLP, correlated with traces via the active span's `trace_id`/`span_id`. The CLI now keys both trace and log export off `OTEL_EXPORTER_OTLP_ENDPOINT`; the `JAZZ_OTEL=1` opt-in and the stdout span-exporter fallback are removed. Stdout continues to emit human-readable text logs.
- 8546306: Added reactive local-first auth helpers for Svelte and Vue, matching `useLocalFirstAuth` in `jazz-tools/react`:
  - **Svelte:** new `LocalFirstAuth` reactive class in `jazz-tools/svelte`. Exposes `secret`, `isLoading`, `login`, and `signOut`; the secret store is read inside `$effect`, so SvelteKit server renders never touch `localStorage`.
  - **Vue:** new `useLocalFirstAuth()` composable in `jazz-tools/vue`. Returns `Ref<string | null>` and `Ref<boolean>` for the secret/loading state plus async `login`/`signOut`; gated on `typeof window` so SSR setup never touches `localStorage`.

  In both frameworks `login`/`signOut` notify every live instance backed by the same store, and a `console.warn` surfaces secret-store failures that previously fell through silently. `BrowserAuthSecretStore` now also throws a clearer error when used outside a browser environment, with `localStorage` resolved lazily so server-side imports of the module-level singleton don't break.

  Note for anyone copying the documented Svelte backup/restore snippets: their signatures changed to take a `LocalFirstAuth` instance (e.g. `createRecoveryPhraseRestore(auth)` returning a callback) so they can call `auth.login()` directly instead of `BrowserAuthSecretStore.saveSecret()` + `location.reload()`.

- c5722fb: `jazzSvelteKit` now starts the managed Jazz dev server and populates `PUBLIC_JAZZ_APP_ID` / `PUBLIC_JAZZ_SERVER_URL` from an `enforce: "pre"` Vite `config` hook, before SvelteKit's `vite-plugin-sveltekit-setup` captures env into `$env/dynamic/public`. The previous approach set these in `configureServer` — after SvelteKit had already frozen its env — and recovered by triggering a fire-and-forget dev-server restart. On a freshly scaffolded starter the first paint reliably rendered with `PUBLIC_JAZZ_SERVER_URL` undefined, and recovery was race-dependent. The restart is removed entirely; the dynamically allocated server URL is now correct on the first request for both cold and warm starts, matching how the Next.js plugin awaits `runtime.initialize()` during config resolution. Plugin order in `vite.config.ts` is no longer load-bearing.
- d86c537: Fix: reject stale transactional writes when the authority sees that a sealed transaction's staged row parents no longer match the current visible row frontier.
- Updated dependencies [e49cf4c]
  - jazz-wasm@2.0.0-alpha.50
  - jazz-rn@2.0.0-alpha.50

## 2.0.0-alpha.49

### Patch Changes

- 24607e1: Disable cold-start replay of persisted rejected batch fates so runtimes no longer surface stale mutation errors or retract visible rows from stored rejection records on startup.
  - jazz-wasm@2.0.0-alpha.49
  - jazz-rn@2.0.0-alpha.49

## 2.0.0-alpha.48

### Patch Changes

- b0a7b14: Fix current runtimes reading stored data written by alpha.46 when tables use the default index configuration.
  - jazz-wasm@2.0.0-alpha.48
  - jazz-rn@2.0.0-alpha.48

## 2.0.0-alpha.47

### Minor Changes

- 3d7e00f: Add edge upstream sync support for self-hosted Jazz servers.

  `jazz-tools server` can now run as an edge when configured with an upstream core URL and peer secret, and the DevServer/testing APIs expose matching upstream and peer-secret options for integration coverage.

### Patch Changes

- 2156a27: Replace replayable batch settlements with whole-batch `BatchFate` sync semantics and remove visible-member manifests from the client-facing fate shape. Successful fate now applies by batch id to locally known rows, avoiding repeated per-row member decoding during subscription settlement.
- 7f7b5da: Add rollback and scoped read support to explicit batches. `DirectBatch`/`DbDirectBatch` now expose `rollback()`, and batch handles can read their own local writes before commit through `DirectBatch.query(...)` and `DbDirectBatch.all(...)`/`one(...)`.
- 6352c68: Make sync batch settlements the durability source of truth, including batch-level rejection fate, settlement-based visibility, transport batching, and more reliable offline replay after reconnect.
- 411d0a0: Updated BetterAuth adapter query-support rules: timestamp columns now support `ne`, `null` filters are only allowed for nullable columns, and `ne null` remains rejected for references and `id`.
- fa3b607: Fix loss of reactivity for query subscriptions with deeply-nested includes. Subscriptions built from depth-2+ `via` include chains (`org.include({ todoViaOrg: { user_checkViaTodo: true } })`) now correctly receive deltas when a row at the bottom of the chain is inserted, updated, or deleted. Previously only the immediate child table was tracked as a dependency of the outer subscription, so mutations further down the chain were silently missed.
- 729effd: Add opt-in development telemetry export for Jazz runtimes and local dev servers. WASM runtimes now buffer spans and logs in Rust only when telemetry is enabled, notify JavaScript through a coalesced drain subscription, and lazy-load client-side OpenTelemetry exporters only after a collector URL is configured.
- 9942d24: Drop the `fetch` polyfill from `jazz-tools/expo`.

  Nothing in jazz-tools consumes a streaming `response.body`, sync runs over WebSocket, and every fetch call site uses buffered methods like `.json()`/`.text()`. The `expo/fetch` swap (and its accompanying `fetchSpecCompliant` URL/Request coercion) was working around `expo/fetch`'s string-only native bridge, but with `expo/fetch` gone, RN's default fetch — which is `whatwg-fetch` under the hood — already accepts URL and Request inputs natively. Better-auth's URL-input path therefore works without the wrapper.

  The `ReadableStream` polyfill stays — it's still consumed by `runtime/file-storage.ts` for the chunked-file API, which Hermes can't service without it.

- 22e5263: Drop no-op `Headers`/`Request`/`Response` polyfills from `jazz-tools/expo`.

  These were imported from `react-native/Libraries/Network/fetch`, which just re-exports `global.*` — so each polyfill collapsed to `globalThis.X = globalThis.X` at best, and to `globalThis.X = undefined` if the polyfill module evaluated before RN installed its networking globals. The latter case broke any consumer that touched `Headers.prototype` (e.g. Clerk's `new Headers(...)` in `fapiClient`), which surfaced as `TypeError: Cannot read property 'prototype' of undefined`. The `fetch` and `ReadableStream` polyfills, which do real work, are kept.

- 37299e9: Keep transaction and direct-batch writes isolated from ordinary indexed queries until commit, while still letting batch-scoped reads see their own staged inserts, updates, and deletes.
- c392b08: Persist authoritative direct batch fate when a server accepts sealed client-originated direct batches, preventing reconnect replays from being answered as missing.
- 30b55f4: Fix IndexedDB-backed storage reads so coalesced disk reads stop at the current file length instead of reading past EOF after uncheckpointed growth.
- 2ea35d5: Fix query subscription sync by preserving local-first auth mode through binary payloads.
- 8962e44: Fix query planning so multiple conditions on the same column are combined correctly, preserving accurate results for same-column where clauses.
- bf27d80: Fix reverse and nested relation includes that selected provenance magic columns such as `$createdAt` and `$updatedAt`.

  Included relation rows now remain present when those magic timestamp columns are selected, instead of resolving as missing, null, or empty because subquery row descriptors dropped non-physical magic columns.

- 5427303: Fix queries with reverse relations that select permission magic columns such as `$canRead`, `$canEdit`, and `$canDelete`.

  Included rows now preserve those permission values for reverse, nested, and recursive relation results instead of dropping the subquery table dependency needed to compute them.

- cd6c2b9: `jazzSvelteKit` from `jazz-tools/dev/sveltekit` now accepts the full Vite `ssr.external` shape (`true | string[]`). Previously the inline parameter type only allowed `string[]`, so `defineConfig({ plugins: [jazzSvelteKit()] })` failed to typecheck under `strict: true`. The `true` sentinel is preserved verbatim to keep externalise-everything semantics.
- 19dc2c4: **Breaking change — action required for Expo / React Native users:** you must now install `jazz-rn` as a direct dependency in every Expo / React Native project (e.g. `npm install jazz-rn` / `pnpm add jazz-rn` / `yarn add jazz-rn`). It used to be pulled in transitively through `jazz-tools`, but is now an optional peer dependency, so it will no longer be installed for you. Web/Node apps are unaffected (jazz-wasm continues to be bundled internally). If `jazz-rn` is missing at runtime, the new `loadJazzRn` loader surfaces an explicit install hint instead of a generic module-resolution error.
- e9bb115: Compress WebSocket transport frame payloads with LZ4 by default.
- 576531d: `ManagedDevRuntime` no longer throws when a prior in-process run leaves `*_JAZZ_SERVER_URL` set in `process.env`. The env var on its own is now treated as our own persisted value and ignored in favour of spinning up a fresh local server. The plugin still takes the "connect to an external server" path when the caller explicitly supplies an `adminSecret` option or sets `JAZZ_ADMIN_SECRET`. This makes Vite HMR restarts and repeated test runs work without stale-state errors.
- fee4160: Switch native targets to `mimalloc` as the global allocator. The `jazz-tools` CLI server binary and the `jazz-napi` Node native module now run on `mimalloc` (via `mimalloc-safe` for napi, the napi-rs–maintained fork). Yields ~~12–26% throughput on alloc-heavy database paths (insert/update/observer) on Linux and macOS without API changes. Bundle-size impact is negligible (~~+43 KB gzipped on the napi `.node`).
- 7f34895: Auto-reload the browser when the schema changes in a Next.js app.
  `withJazz` writes the live schema hash into a generated module that the
  React provider depends on, so Turbopack and Webpack reload the page
  whenever the schema is pushed — no consumer-side wiring required.
- e5c83ea: Keep dev plugins running when the initial schema auto-push cannot reach a configured remote Jazz server. The plugins now warn and continue with the configured app and server URL, while still failing on schema, auth, or server rejections.
- 670f797: fix: prevent null field updates from getting lost on sync
- 1d1bdc7: Skip catalogue replay from clients that are not authenticated with catalogue publish authority, avoiding harmless `CatalogueWriteDenied` sync errors.
- 5b1d352: Stop server row replay from sending local durability acknowledgements back upstream, and ignore client-sent durability acknowledgements as authoritative server state.
- 5752bde: Add React Native anonymous auth support. `jazz-tools` now mints anonymous JWTs through the React Native runtime module when no auth credentials are provided, and `jazz-rn` exposes the matching native `mintAnonymousToken` binding.
- 15b347d: `jazz-rn`: `query` is now `async` and no longer blocks the React Native JS thread on one-shot reads.

  The native uniffi export used `block_on` on the JS thread, so any `db.all(...)` that needed a later `batched_tick` to settle (e.g. queries that wait on server-sourced data or parked sync messages) could deadlock — the JS thread was blocked, so the `batched_tick` callback could never fire to fulfil the query future. The export is now `async fn` and uniffi-bindgen-react-native generates a Promise-returning JSI call, polled off the JS thread. `JazzRnRuntimeAdapter.query` now `await`s the binding before parsing.

- fe0c43a: Move the dedicated-worker bridge orchestration (init handshake, peer routing, lifecycle hints, shutdown handshake, outbox routing, server-payload forwarding) out of TypeScript and into Rust (`crates/jazz-wasm`). `JsSyncSender` and the `onSyncMessageToSend` WASM API are gone; the runtime now posts directly to the worker via `worker.postMessage`. The TypeScript `WorkerBridge` is a thin adapter over the Rust-owned `WasmWorkerBridge`; `jazz-worker.ts` is reduced to a WASM-bootstrap shim. Public `Db` API and the on-the-wire structured-clone protocol shape are unchanged.
- 181a66f: Modifies `createPolicyTestApp` to receive an app and permission objects instead of the schema's dir.
- 92fbdf9: Persist sealed batch manifests and batch fates instead of replayable local batch records. Batch waits and mutation-error replay now read `BatchFate` directly, and sync no longer rebuilds local batch membership one row at a time.
- e336d8d: Modify the permission tests API to make `expectAllowed`/`expectDenied` side-effect free.
- 57a21cd: Fix `s.timestamp()` row output inference so timestamp columns are typed as `Date` instead of `Date | number`.

  Numeric timestamp defaults remain accepted, but inserted and queried rows now match the runtime shape and infer timestamp values as JavaScript `Date` objects.

- a523693: Add `rollback()` to transaction handles. Calling rollback closes the transaction without committing it, so later writes, reads, commits, or rollbacks on the same transaction fail.
- Updated dependencies [2156a27]
- Updated dependencies [6352c68]
- Updated dependencies [fa3b607]
- Updated dependencies [729effd]
- Updated dependencies [e9bb115]
- Updated dependencies [5752bde]
- Updated dependencies [15b347d]
- Updated dependencies [92fbdf9]
  - jazz-wasm@2.0.0-alpha.47
  - jazz-rn@2.0.0-alpha.47

## 2.0.0-alpha.46

### Patch Changes

- f528f1a: Send direct-batch durability confirmations once per sealed batch instead of once per row.
- 76a3b89: Filter replayed batch settlement members to each receiving client's scope.
  - jazz-wasm@2.0.0-alpha.46
  - jazz-rn@2.0.0-alpha.46

## 2.0.0-alpha.45

### Patch Changes

- 38cfc0e: Improve local write performance by skipping idle subscription recompilation, reducing local batch member sorting, and reusing prepared schema insert alignment.
- 8fd0db9: Gate tiered browser subscriptions so the first callback is held until the worker bridge has replayed the settled server snapshot instead of exposing an empty transient snapshot.
- 2ee98be: Add sync protocol version checks to the WebSocket handshake so incompatible clients and servers fail with an explicit update prompt.
- 7b01f3f: Improve local insert performance by reusing prepared write context through row-history application and caching catalogue row descriptors for repeated same-schema writes.
- 3a6251b: Avoid duplicate server subscription authorization checks when computing authorized sync scope.

  Thanks, Tobi!

- a065e63: Skip settling clean query subscriptions when no graph, visibility, or local-update state changed.

  Thanks, Tobi!

- 75aa36b: Add `window.__jazz.shutdown(namespace?)` for awaiting Jazz client teardown (worker termination + IndexedDB lock release). Useful in browser tests that mount and unmount apps between cases. `Db.shutdown()` is now idempotent — concurrent or repeated calls share the same in-flight promise — so the new API plays cleanly alongside framework-driven cleanup (e.g. JazzProvider unmount).
- Updated dependencies [8fd0db9]
- Updated dependencies [2ee98be]
  - jazz-wasm@2.0.0-alpha.45
  - jazz-rn@2.0.0-alpha.45

## 2.0.0-alpha.44

### Patch Changes

- e5c9441: Memoize runtime schema cache keys by schema object identity so repeated writes do not reserialize the full schema on every `Db.getClient` lookup.
- 53a4d4c: Optimize paginated query settling while preserving authorized sync scope for offset/limit queries.
  - jazz-wasm@2.0.0-alpha.44
  - jazz-rn@2.0.0-alpha.44

## 2.0.0-alpha.43

### Patch Changes

- bca70a5: - Transactions and batches no longer require a table when being created. The schema is determined by the first CRUD operation performed inside the transaction.
  - Add new `Db.transaction` and `Db.batch` methods. These methods receive a callback as parameter and commit automatically once the callback finishes running. They are preferred over their `beginTransaction`/`beginBatch` counterparts.
- 51cbb09: Fix `jazz-tools/expo` fetch polyfill rejecting URL and Request inputs.

  `expo/fetch`'s native bridge only accepts a string for the first argument, so calling `fetch(new URL(...))` or `fetch(new Request(...))` — both valid per the WHATWG spec, and the form better-auth's client uses — failed with "The 2nd argument cannot be cast to type URL". The polyfill now normalises URL and Request inputs to a string URL plus a merged init (including the request body) before delegating to `expo/fetch`.

- b092cec: The `jazz-tools` CLI now loads `.env` from the working directory at startup, so `deploy` and other commands pick up `JAZZ_ADMIN_SECRET`, `JAZZ_SERVER_URL` (and framework-prefixed equivalents) from the same dotenv file your app uses. Pass `--env-file <path>` (repeatable) to load from a specific file — useful for staging/production splits like `--env-file .env.staging`. Real environment variables still take precedence.
- e71f3f0: Fix `jazzPlugin()` (`jazz-tools/dev/vite`) so its return type matches Vite's `Plugin`. The `config` hook's parameter previously typed `ssr.external` as `string[] | undefined`, but Vite's `UserConfig` allows `true | string[] | undefined` (`true` = externalize everything), causing `TS2769` in consumer `vite.config.ts` files. Widen the param and preserve `external: true` when the user already opts into it.
- bb60ab4: Fix MaterializeNode passing an empty table name to the row loader, which caused old-branch rows to be silently dropped after a schema migration on storage backends that resolve rows by locator. Apps with persistent local data from a previous schema would see all their old rows disappear from query results until a fresh sync.
- c1c873f: Speed up per-row history lookups in `MemoryStorage` by nesting the history map by `row_id`, so reads scale with the row's own history rather than the table size.
- 3a9726e: Trigger a Vite full-reload from the Jazz Vite and SvelteKit dev plugins
  whenever the schema watcher successfully pushes an updated schema, so the
  browser picks up the new schema without a manual refresh.
- 31c9562: Remove the unused `insertDurable` / `insertDurableWithSession` / `updateDurable` / `updateDurableWithSession` / `deleteDurable` / `deleteDurableWithSession` methods from the `Runtime` interface and from the jazz-napi, jazz-wasm, and React Native runtime adapters. These were superseded by the `insert(...).wait({ tier })` / `update(...).wait({ tier })` / `delete(...).wait({ tier })` API and had no remaining callers.
- 826d1bc: Fix `getLocalFirstIdentityProof` on React Native by minting the proof token through `jazz-rn` instead of the (unavailable) WASM module, restoring the local-first → upgrade-to-account sign-up flow on RN.
- 3e2564a: Queue unsent parent row batches before child row batches when syncing to servers. This keeps server-side permission checks from evaluating updates before their prior row content has arrived.
- fafdd2c: Align batch and transaction writes with the simple write API by supporting custom insert ids and upserts.
  - jazz-wasm@2.0.0-alpha.43
  - jazz-rn@2.0.0-alpha.43

## 2.0.0-alpha.42

### Patch Changes

- 3ebf45c: Deduplicate permissions bundle publishes when content is unchanged.

  `publish_permissions_bundle` now short-circuits when the proposed `schema_hash` and `permissions` are identical to the current head, mirroring the content-addressed dedup that schemas already enjoy. Repeated identical publishes no longer bump the version, allocate a new bundle object id, or rewrite the catalogue entry. Optimistic concurrency via `expected_parent_bundle_object_id` still rejects stale parents.

- 02b0f64: Allow cookie-backed WebSocket auth between loopback hosts on different ports.
  This keeps cookie auth same-origin by default, but treats `localhost`,
  `*.localhost`, `127.0.0.1`, and `::1` as trusted local development peers.
  - jazz-wasm@2.0.0-alpha.42
  - jazz-rn@2.0.0-alpha.42

## 2.0.0-alpha.41

### Patch Changes

- 7ed02fc: Add `schema.defineSliceableApp(schema).slice(...)` for deriving smaller typed app surfaces backed by one complete runtime schema.
- 6585b53: Add transformed columns to the TypeScript schema DSL.

  Column definers like `s.string()`, `s.boolean()`, and `s.int()` now support `.transform({ from, to })`, allowing apps to expose a transformed TypeScript value while storing the underlying column's normal SQL type. Transforms apply on rows returned from reads and subscriptions, and in reverse before inserts and updates.
  - jazz-wasm@2.0.0-alpha.41
  - jazz-rn@2.0.0-alpha.41

## 2.0.0-alpha.40

### Patch Changes

- bca35a7: Use output schema from JSON standard schema for the stored schema of `json` columns
- 3e9780a: `jazz-tools` CLI now accepts framework-prefixed env vars (`PUBLIC_`, `VITE_`, `NEXT_PUBLIC_`, `EXPO_PUBLIC_`) as fallbacks for `JAZZ_SERVER_URL` and `JAZZ_APP_ID`, matching the names the SvelteKit, Vite, Next.js and Expo plugins already write. `JAZZ_ADMIN_SECRET` remains unprefixed.
- 72e8727: Docs/example: document the correct Expo setup for `jazz-tools`.

  `jazz-tools` emits `import.meta.url` from its runtime, so Expo apps must enable `unstable_transformImportMeta` in `babel-preset-expo` or Hermes fails to parse the bundle. `@expo/metro-config` only auto-detects `.babelrc`, `.babelrc.js`, and `babel.config.js` — `.cjs`/`.mjs` variants are silently ignored, and `config.transformer.extendsBabelConfigPath` is a no-op on Expo's pipeline. The Expo install docs and the `todo-client-localfirst-expo` example now use a plain CJS `babel.config.js` with `unstable_transformImportMeta: true`, drop the stray `.babelrc` shim, and stop declaring `"type": "module"`. `metro.config.mjs` stays ESM so it can top-level `await withJazz(...)`.

- 4f017af: Honor the root `adminSecret` option in `ManagedDevRuntime` (used by the `vite`, `next`, `sveltekit`, and `expo` dev plugins). The managed local-server branch previously read only `server.adminSecret` and silently fell back to a random `jazz-dev-XXXXXXXX` when the root option was set; it now falls through to root `adminSecret`, mirroring the precedence already used for `appId`. The startup banner also surfaces the resolved admin secret, and the Expo plugin now logs the inspector link the same way `vite` and `next` do.
- c882c89: Fix `$createdAt` and `$updatedAt` provenance magic columns in `jazz-tools` so they round-trip as real JavaScript dates instead of far-future timestamps.

  Queries now convert JS millisecond `Date` and numeric filter values to the internal provenance timestamp format consistently, so selecting and filtering on those magic columns uses the same time scale.

- 19ab5c4: Fix JazzProvider re-initialising when an inline config object is passed on every render. The useEffect dep array previously included `config` (the object reference); it now uses `configKey` (the JSON-stringified value) so that structurally identical config objects no longer trigger a cleanup→reacquire cycle.
- c5f0807: Wrap React `use` inside `JazzClientProvider` so it remains compatible with React 18 while preserving behavior on newer React versions.
- 0103d1c: Fix the SvelteKit dev plugin so the first-ever cold start no longer needs a manual restart. The plugin now triggers a Vite restart immediately after allocating a fresh app ID, so SvelteKit's `$env/*` capture re-reads the now-populated `.env` on the second pass. Plugin order in `vite.config.ts` is no longer load-bearing.
- 9329dbe: `jazzPlugin` now adds `jazz-napi` to `ssr.external` so that SSR builds on Vite-based frameworks (e.g. SvelteKit) don't attempt to bundle the native add-on. Also tightens error handling in `persistAppIdToEnv` to only swallow `ENOENT`; other I/O errors now propagate correctly.
- 3ea24ed: Fixed the flood of ws messages by stop forwarding client-origin `RowBatchStateChanged` acknowledgements to other subscribers. This keeps per-client row-batch durability bookkeeping local to the server/client pair and avoids leaking `BatchSettlement` echoes to unrelated WebSocket clients.
- ff2eb8a: Preserve column defaults and counter merge strategies when loading typed-app schemas through the CLI schema loader, keeping the AST and wasm schema round-trip lossless for fields both representations support.
- acc7d89: Fix native WebSocket TLS handshakes failing on mobile (Expo/React Native) when the OS root certificate store is empty or unavailable, by falling back to bundled `webpki` roots only when no native roots are loaded.
- f0260e0: Prevent TokioScheduler task pileup under contention.

  `schedule_batched_tick` used to clear the debounce flag at the start of the spawned task, before acquiring the core lock. When the lock was held, every caller arriving during that window saw `scheduled=false` and spawned another task, piling up behind the same mutex. The flag is now cleared after the lock is acquired, immediately before `batched_tick` runs, capping the pending queue at one tick while preserving the lost-wakeup fix.

- bee22ff: `QuerySubscription` in the Svelte bindings now accepts a getter for both the query and the options, e.g. `new QuerySubscription(() => filter ? app.todos.where({ title: { contains: filter } }) : undefined)`. Reactive reads inside the getter are tracked, so the subscription re-runs when its dependencies change. The bare-value forms continue to work unchanged.
- 402d104: `jazzPlugin` and `jazzSvelteKit` now alias `jazz-wasm` to an absolute path resolved from `jazz-tools`'s own install location. This removes the need for Vite/SvelteKit consumers on pnpm to add `jazz-wasm` as a direct dependency just to work around pnpm's strict-isolation layout.
  - jazz-wasm@2.0.0-alpha.40
  - jazz-rn@2.0.0-alpha.40

## 2.0.0-alpha.39

### Patch Changes

- 94bd2b8: Update Jazz server URL in generated starter apps to https://v2.sync.jazz.tools/.
- 2a3545c: Stop replaying upstream-confirmed rows to the server on reconnect. The client's full-storage sync now skips rows whose `confirmed_tier` is already above the node's own tier, so a user-role client no longer re-pushes subscription-delivered rows it never authored. Previously these replays were rejected by row-level update policies (e.g. "Update denied by USING policy — cannot see old row") on every reconnect.
  - jazz-wasm@2.0.0-alpha.39
  - jazz-rn@2.0.0-alpha.39

## 2.0.0-alpha.38

### Patch Changes

- a4453e0: `jazzPlugin` and `jazzSvelteKit` now inject `worker.format: "es"` and `optimizeDeps.exclude: ["jazz-wasm"]` via a Vite `config` hook. Consumers no longer need to set these manually in their `vite.config.ts`.
  - jazz-wasm@2.0.0-alpha.38
  - jazz-rn@2.0.0-alpha.38

## 2.0.0-alpha.37

### Patch Changes

- c825970: Re-apply stored Rejected batch settlements on runtime startup so that a crash between persisting a rejection and deleting its visible row no longer causes the lingering row to flash into queries on reload before being retracted.
- a4b83ea: `jazz-tools deploy` now warns about tables that have no explicit permission policy, matching the behaviour of `jazz-tools validate`.
- f8981c6: Fix `jazz-tools deploy` so apps without a `permissions.ts` file still publish their structural schema. The CLI now skips the permissions publish step instead of failing when no current permissions are defined.
- 751eff9: Fall back to ephemeral in-memory storage when IndexedDB is blocked by a SecurityError (Firefox private browsing, Safari private mode). Jazz now initialises successfully without persistence instead of failing to load entirely.
- 961361c: Treat reconnect row-history replay as idempotent only when the incoming row exactly matches the stored history member. This avoids spuriously reclassifying replayed inserts as updates on insert-only tables while still allowing same-batch corrections to propagate their final payload.
- 0fc5388: Add `jazz-tools schema hash` command to print the short hash of the current `schema.ts` without hitting the server or writing a local snapshot.
- efa67bf: Admin-secret clients now bypass local row-policy enforcement so writes still reach the sync server, where permissions are actually checked.
- fd3bd95: `Transaction.commit()` now returns a write handle, which allows waiting for ack from a given durability tier
  - jazz-wasm@2.0.0-alpha.37
  - jazz-rn@2.0.0-alpha.37

## 2.0.0-alpha.36

### Patch Changes

- Cache runtime schema lookups across query paths and surface unhandled rejected mutations through targeted batch-id queues instead of rescanning every retained local batch record. This also brings the React Native runtime onto the same rejected-batch helper surface as WASM and N-API.
- 8bb9fbc: Allow caller-supplied row ids to use any valid UUID and rely on explicit row metadata for created-at semantics.
- 3578b10: Stop persisting and rehydrating a bogus empty schema on dynamic-schema servers.

  `SchemaManager::new_server` leaves the context uninitialized with a sentinel hash. Runtime construction then called `ensure_current_schema_persisted`, writing a placeholder `catalogue_schema` row whose content hashed to the empty-schema digest. On rehydrate that hash surfaced as an "unreachable schema hash" in every connection diagnostics call. The persist path now no-ops while uninitialized, and `process_catalogue_update` ignores empty schemas for forward-compatibility with sqlite files written by the pre-fix server.

- 34e9ca4: Add a browser `window.__jazz.clearStorage()` helper for framework clients, and move the FAQ to a top-level docs page with updated storage-reset guidance.
- Updated dependencies
- Updated dependencies [8bb9fbc]
  - jazz-wasm@2.0.0-alpha.36
  - jazz-rn@2.0.0-alpha.36

## 2.0.0-alpha.35

### Minor Changes

- 9067b0c: Add static external JWT verification alongside JWKS-based verification in `jazz-tools`.

  The Rust server CLI now accepts `--jwt-public-key` / `JAZZ_JWT_PUBLIC_KEY`, and the TypeScript backend `createJazzContext(...)` path now accepts `jwtPublicKey`. Both server entrypoints reject configs that set both `jwksUrl` and the new static-key option at the same time.

### Patch Changes

- 4d67804: Promote `authMode` to a first-class typed field, add anonymous auth, and overhaul the React provider.
  - **`Session.authMode`** is now `"external" | "local-first" | "anonymous"`, derived from the JWT `iss` claim instead of an opaque string in `claims`. The permissions DSL exposes `session.authMode` and supports `session.where({ authMode })`.
  - **Anonymous auth**: when `DbConfig` has neither `secret` nor `jwtToken`, the client mints an ephemeral token. Anonymous sessions can read but are structurally denied writes (checked before policy evaluation); failures surface as `AnonymousWriteDeniedError` on the client.
  - **`DbConfig` flattened**: `auth: { localFirstSecret }` → `secret`.
  - **`AuthState` flattened**: `{ authMode, session, error? }` — no more `status` / `transport` union.
  - **React provider**: `JazzProvider` uses Suspense + `React.use()`; new `onJWTExpired` prop serializes refresh calls and replaces custom sync-wrapper components. New `useAuthState()` and `useLocalFirstAuth()` hooks. Context carries only `{ client }`.
  - **Identity module**: `mint_local_first_token` / `verify_local_first_identity_proof` → `mint_jazz_self_signed_token` / `verify_jazz_self_signed_proof`, taking an explicit issuer.

- 75756a6: Standardize BatchId JSON values on hex strings across Jazz write-context bindings.

  Batch write contexts now accept the TypeScript wire shape used by current clients, including lowercase `batch_mode` values and string `batch_id` values, and reject the old array-style BatchId JSON representation.

- e5a3189: Add schema-level per-column merge strategies to `jazz-tools`.

  Columns now default to MRCA-relative per-column LWW, and non-nullable integer columns can opt into `merge("counter")` to merge concurrent snapshots by summing their MRCA-relative deltas. Merge strategy is schema metadata, so different schema versions can resolve the same conflicting history differently without rewriting stored rows.

- 772ce14: Require a published permissions head before session-scoped writes can rely on backend authority. Persisted writes against enforcing backends without a current permissions head now reject explicitly with `permissions_head_missing`, and synced-query tests now publish permissions before expecting backend-visible rows or cross-schema authorization results. Session-scoped queries still withhold authoritative remote scope before a permissions head exists, but explicit query/subscription rejection is deferred for now.
- 947f362: Simplify external JWT identity: `session.user_id` is now the JWT `sub` claim verbatim. The `jazz_principal_id` claim, the `external_identities` server mapping, and the hashed `external:…` fallback are removed. External providers must emit the desired Jazz user id as `sub` directly (e.g. via `getSubject: ({ user }) => user.id`). Also fixes `authMode` resolution in the policy evaluator and preserves `AnonymousWriteDeniedError` through the runtime write path.
- caad318: Modify write APIs to return a `WriteHandle`, which allows callers to wait for a given durability tier to acknowledge the write or reject it. Also introduces a global `onMutationError` handler to receive errors that aren't explicitly handled with `WriteHandle.wait`.
- 45be93a: `jazz-tools` now routes its sync, schema, permissions, migration, and introspection requests under app-scoped server paths like `/apps/<appId>/...` instead of relying on a configurable `serverPathPrefix`. Server-backed CLI commands now take `<appId>` when resolving those endpoints.
- Updated dependencies [4d67804]
- Updated dependencies [75756a6]
- Updated dependencies [947f362]
- Updated dependencies [caad318]
  - jazz-wasm@2.0.0-alpha.35
  - jazz-rn@2.0.0-alpha.35

## 2.0.0-alpha.34

### Patch Changes

- 0585935: Fix IndexedDB B-tree page splitting for large index keys by choosing split points based on encoded page size instead of entry count. This prevents synced inserts with many near-threshold JSON index values from failing with leaf or internal split fit errors.
- 213288a: Bind qualified `where(...)` filters on hopped permission relations to the relation's actual joined scope so correlated `exists(...)` closures over gathered team grants evaluate correctly at runtime.
- 66dc47a: Direct write conflicts now resolve with MRCA-based per-column LWW for visible merge previews and merge-on-write rebases, including accepted transactional rows.

  Visible rows also persist compact winner provenance ordinals so tier-aware reads can reuse merged previews without re-walking row history when tiers have already converged.
  - jazz-wasm@2.0.0-alpha.34
  - jazz-rn@2.0.0-alpha.34

## 2.0.0-alpha.33

### Patch Changes

- jazz-wasm@2.0.0-alpha.33
- jazz-rn@2.0.0-alpha.33

## 2.0.0-alpha.32

### Patch Changes

- 2d10b2e: Include the failing index column in synced insert index-update error logs so IndexedDB-backed index failures are easier to diagnose.
  - jazz-wasm@2.0.0-alpha.32
  - jazz-rn@2.0.0-alpha.32

## 2.0.0-alpha.31

### Patch Changes

- ea68566: Isolate browser-local Jazz persistence by default across users and app scopes.

  `createDb()` now derives the default browser persistent namespace from both `appId` and the resolved authenticated principal when no explicit `dbName` is provided, preventing one user from reopening another user's IndexedDB-backed local cache. `BrowserAuthSecretStore` also now accepts scope hints like `appId`, `userId`, and `sessionId` so browser apps can avoid sharing one global local-first identity secret across unrelated sessions.

- 44b90c0: Fix misleading schema-mismatch recovery guidance during client/server handshakes.

  The transport handshake now sends the client's declared structural schema hash separately from the catalogue-state digest, so server-side connection diagnostics only suggest migrations for real schema hashes that the CLI can resolve.

- 50e46c0: Add HttpOnly cookie auth support to `jazz-tools` with a mirrored browser
  `cookieSession` for local permission evaluation. Servers can now accept JWT auth
  from a configured auth cookie, and cookie-backed websocket handshakes are
  restricted to same-origin requests.
- fd98d6e: Add coordinated browser logout and storage wipe support so follower tabs can trigger an IndexedDB reset through the elected leader, stale fallback namespaces are removed, and `db.logout({ wipeData: true })` clears browser state before the next session starts.
- 09e16b4: Support recursive gather seeds built from composed same-table relations, including hop-based and unioned permission closures.
- 2e8e918: Cap oversized secondary index keys to a 5 KiB budget so large text values still use the truncate-and-hash encoding without producing IndexedDB index entries that can overflow B-tree page splits.
- 05649ae: Add a new `deploy` CLI command to upload the current schema and permissions to the server. Replaces the existing `permissions push` command.
- 80a0360: Add `updatedAt` overrides to `insert`, `update`, and `upsert` mutation options in `jazz-tools`.

  The same override is available on the durable variants, so callers can stamp `$updatedAt` explicitly on a per-write basis without changing attribution or session scoping.

- 11921e6: Use static `new URL(...)` import for worker when no explicit runtime sources are configured, allowing bundlers (Turbopack, webpack, Vite) to detect and co-bundle the worker script and its WASM dependency automatically.

  Also passes a computed `fallbackWasmUrl` in the worker init message so non-bundled (static HTML) deployments still receive an explicit WASM path as a last resort if `wasmModule.default()` fails.

- Updated dependencies [09e16b4]
  - jazz-wasm@2.0.0-alpha.31
  - jazz-rn@2.0.0-alpha.31

## 2.0.0-alpha.30

### Patch Changes

- 848e94d: Replace HTTP `/sync` + SSE `/events` with a single Rust-owned WebSocket `/ws` transport, and wire JWT rotation and server-side auth rejection end-to-end across all bindings.

  **Transport rewrite**
  - New `TransportManager` in `jazz-tools` owns the WebSocket connection, framing (4-byte big-endian length + JSON), reconnect with exponential backoff, periodic heartbeat, and a `TransportControl` channel that observes `Shutdown` / `UpdateAuth` in every phase (connect, backoff, handshake, connected). Dropped `TransportHandle` triggers an implicit shutdown.
  - New `install_transport` helper on `RuntimeCore` centralises the boilerplate (create manager → seed catalogue state hash → register handle → spawn) so all four bindings converge on one code path.
  - `NativeWsStream` (tokio-tungstenite + rustls) and `WasmWsStream` (ws_stream_wasm) implement the shared `StreamAdapter` trait.

  **Auth refresh**
  - `JazzClient.updateAuthToken(jwt)` now pushes the refreshed credentials into the live transport via `runtime.updateAuth`, which routes through `TransportControl::UpdateAuth` and triggers a reconnect with the new auth. Previously the call only mutated local context.
  - `ConnectSyncRuntimeOptions.onAuthFailure` is now wired to `runtime.onAuthFailure` and fires whenever the server rejects the WS handshake with `Unauthorized`. NAPI exposes a `ThreadsafeFunction`-based callback; React Native exposes a UniFFI `callback_interface`; WASM keeps its existing `Function` callback.
  - The worker posts `auth-failed` back to the main thread when `runtime.updateAuth` throws, and supports `update-auth` / `disconnect-upstream` / `reconnect-upstream` messages from the bridge.

  **Bindings**
  - WASM, NAPI, and React Native all now expose `connect`, `disconnect`, `update_auth`, `on_auth_failure`.
  - React Native: `JazzRnRuntimeAdapter` forwards `updateAuth` and `onAuthFailure` to the UniFFI binding (previously missing — auth refresh was a silent no-op on RN).
  - `JazzClient.updateAuthToken` carries `admin_secret` and `backend_secret` forward from context (previously the serialised payload only included `jwt_token`, silently erasing privileged credentials on every refresh).

  **Breaking changes**
  - `POST /sync` and `GET /events` HTTP routes are deleted; external callers receive 404. Use the WebSocket `/ws` route via `runtime.connect(url, authJson)`.
  - `RuntimeCore<S, Sch, Sy>` is now `RuntimeCore<S, Sch>` — the `SyncSender` generic parameter has been removed.
  - `NapiSyncSender` and `RnSyncSender` are removed; bindings use `runtime.connect` instead.
  - `TokioRuntime::new` no longer takes the trailing `SyncSender` argument.
  - Cargo features `transport` and `transport-http` are removed; transport types are default-on, and `transport-websocket` enables the WS implementation.

  **Tests**
  - Inline `TransportManager` tests cover shutdown and `update_auth` in every phase (connect / backoff / handshake / connected).
  - Re-enabled two previously-`#[ignore]`d sync-reliability tests after fixing a debounce-flag race in `TokioScheduler::schedule_batched_tick`.
  - Integration tests migrated from `/events` to `/ws`.
  - New TS coverage: `client.test.ts` (onAuthFailure wiring + secret preservation), `napi.auth-failure.test.ts` (E2E against real NAPI + server), `db.transport.test.ts`, `url.test.ts`, `db.auth-refresh.worker.test.ts` (browser worker round-trip), RN adapter tests for `updateAuth` / `onAuthFailure`.

- e346057: Fix React Native cold-start on offline and unblock initial subscriptions when the transport can't reach the server.
  - `jazz-rn` now regenerates its UniFFI bindings for the `insert` / `insert_with_session` signatures introduced with the caller-supplied UUIDv7 APIs, so the native library and JS adapter agree at startup and Jazz initializes in the app.
  - `jazz-rn` now calls `rehydrate_schema_manager_from_catalogue` after opening SQLite, matching the WASM runtime, so offline cold-starts can decode previously-persisted rows against their original schema/permissions history.
  - `jazz-tools` bounds the "hold remote query frontier while transport connects" wait so a never-completing transport no longer stalls first subscription delivery forever. Pending servers now clear on a new `TransportInbound::ConnectFailed` event (fired from the connect/handshake error paths in both tokio and wasm run loops), with a 2s safety-net timeout for hung connects. The frontier hold also re-evaluates live at settle time so offline or hung first-connect cases release immediately once pending clears.

  No public-API break. RowPolicyMode selection, persisted-row wire format, and transport handshake semantics are unchanged.

- b8581ad: The standalone inspector now shows each table's currently published sync-server permissions alongside its stored schema, making it easier to verify the rules your server is actively enforcing.
- Updated dependencies [5c80e4d]
- Updated dependencies [848e94d]
- Updated dependencies [e346057]
  - jazz-wasm@2.0.0-alpha.30
  - jazz-rn@2.0.0-alpha.30

## 2.0.0-alpha.29

### Patch Changes

- 58ace62: Add external UUIDv7 create APIs and id-based upsert APIs across the Rust and TypeScript client surfaces.
- Updated dependencies [58ace62]
  - jazz-wasm@2.0.0-alpha.29
  - jazz-rn@2.0.0-alpha.29

## 2.0.0-alpha.28

### Minor Changes

- f6d18f8: Add BIP39 recovery phrase for local-first identity, exposed at the new `jazz-tools/passphrase` subpath. `RecoveryPhrase.fromSecret` / `RecoveryPhrase.toSecret` encode and decode the 32-byte local-first auth secret as a 24-word English mnemonic, with structured `RecoveryPhraseError` codes and forgiving whitespace/case normalization. Also fixes a latent cache bug in `BrowserAuthSecretStore` and `ExpoAuthSecretStore` where `saveSecret` did not invalidate `cachedPromise`, so a restore after `getOrCreateSecret` would silently keep the pre-restore secret.

### Patch Changes

- 6b2ceff: Reduce migration workflow churn for schema changes that do not require row transforms.

  `jazz-tools migrations create` and `jazz-tools migrations push` now treat default-only and column-order-only schema hash changes as compatible transitions that do not need a reviewed migration file, while still requiring reviewed migrations for incompatible changes like nullability or reference updates. The CLI also now accepts reviewed migration modules that load through CommonJS-style nested `default` exports.

- 6afc27e: Fix row-level policies that reference a row's own `id`, including claim checks like `id IN @session.claims.editable_doc_ids`, so write permissions evaluate against the row `ObjectId` even when the table has no explicit `id` column.
- 9b45ec5: Adopt the new row-permission strategy across client and server runtimes. Local clients that only have a structural schema stay permissive for offline reads and writes, while runtimes with current permissions and sync servers enforce deny-by-default row access for session-scoped reads, inserts, updates, and deletes.
- 234b138: Added `jazzSvelteKit()` Vite plugin (`jazz-tools/dev/sveltekit`) for SvelteKit and Vite+Svelte projects. Starts an embedded Jazz dev server, publishes and watches the schema, and injects `PUBLIC_JAZZ_APP_ID`/`PUBLIC_JAZZ_SERVER_URL` into the Vite env. Supports three modes: embedded local server (default), connect to an explicit URL via `server: "https://…"`, or connect to a server already described in `PUBLIC_JAZZ_SERVER_URL`. Defaults `schemaDir` to `src/lib/` to match SvelteKit conventions.
- e752ae2: Remove demo auth, anonymous auth, and synthetic users. The only valid auth modes are now local-first (Ed25519 JWT) and external (JWKS JWT). Add Expo support for local-first auth secret generation via expo-crypto.
- 4792880: Verify bearer JWTs inside backend `createJazzContext(...).forRequest()` / `withAttributionForRequest()`, add backend `jwksUrl` and `allowSelfSigned` config, and share JWT-to-session mapping with the runtime session helpers. These request-scoped backend helpers are now async so callers can await self-signed or JWKS-backed verification.
- Updated dependencies [9b45ec5]
  - jazz-wasm@2.0.0-alpha.28
  - jazz-rn@2.0.0-alpha.28

## 2.0.0-alpha.27

### Patch Changes

- d872a4d: `allowedTo` now accepts bare relation names (e.g. `"project"`) in addition to full FK column names (`"projectId"`).
- cfaed19: Fix enum literals in nested policies

  Nested relation-backed permission filters now serialize enum literals as tagged runtime values instead of raw strings, so publishing permissions and loading them into `createJazzContext(...)` works for cases like `grant_role: "viewer"`.

- 1fb1395: Add `From<T>` impls on `Value` for common types and a `row_input!` macro for ergonomic `HashMap<String, Value>` construction.
- 463098a: Ship the new unified row-history storage engine across Jazz runtimes.

  Relational rows, query visibility, and sync replay now go through the same storage-backed path instead of mixing durable state with older in-memory cache layers. In practice this makes local persistence and sync behavior more consistent across browser, Node, and native runtimes, especially around cold start, reconnect, and large local datasets.

- Updated dependencies [463098a]
  - jazz-wasm@2.0.0-alpha.27
  - jazz-rn@2.0.0-alpha.27

## 2.0.0-alpha.26

### Patch Changes

- 15ce77e: Fix large global query and subscription snapshots dropping rows by sequencing sync delivery and delaying `QuerySettled` tier unlocks until earlier sync updates have been applied.
- 5a2adfd: Fix `state_referenced_locally` compiler warnings in Svelte components by moving prop reads into reactive contexts.
- 8be5761: Add `AddTable`, `RemoveTable` and `RenameTable` migrations
- 75b30a9: feat: enhance inspector with inline editings, resizeable panels and a shiny new grid
- 75b30a9: Fix the inspector data grid freezing the browser tab when paging or sorting, and improve diagnostics around pending query transitions.
- 9968d2f: Fix `createPolicyTestApp(...)` so policy test helpers no longer hard-code Vitest's `expect`.

  Callers now pass the `expect` function explicitly, which keeps `jazz-tools/testing` policy assertions working when the test harness provides its own assertion context.

- 6bb5d9f: Add a `runtimeSources` client config API for explicit Wasm and worker bootstrap across browser and edge-style runtimes, including `baseUrl`, `wasmUrl`, `workerUrl`, `wasmSource`, and `wasmModule` overrides exported from the runtime and framework entrypoints.
- d302911: Allow `QuerySubscription` (Svelte) and `useAll` (Vue) to accept `undefined` queries, matching the React `useAll` behaviour. When `undefined` is passed, the subscription returns `undefined` without subscribing.
- 4d57125: Fix schema comparison in `permissions push` CLI command
- Updated dependencies [a1cb9d5]
  - jazz-rn@2.0.0-alpha.26
  - jazz-wasm@2.0.0-alpha.26

## 2.0.0-alpha.25

### Patch Changes

- 30df2b4: Fix browser worker reconnect after network loss when offline `local`-tier writes were queued locally.

  The worker now aborts its stale upstream events stream before scheduling reconnect after sync POST failures, which lets later writes promote normally once network access returns. This also adds browser regression coverage for the split-context reconnect case where one client stays online while another writes offline and reconnects.
  - jazz-wasm@2.0.0-alpha.25
  - jazz-rn@2.0.0-alpha.25

## 2.0.0-alpha.24

### Patch Changes

- 08f10b9: CLI schema resolution now accepts apps that keep `schema.ts` and `permissions.ts` in `src/` as well as the app root.

  The legacy `--schema-dir ./schema` shim is no longer supported. Point CLI commands at the app root instead, where Jazz will resolve `schema.ts` from either the root or `src/`.
  - jazz-wasm@2.0.0-alpha.24
  - jazz-rn@2.0.0-alpha.24

## 2.0.0-alpha.23

### Patch Changes

- d1d19a5: Allow development-mode clients to auto-publish the current structural schema from `schema.ts` without an admin secret, while keeping non-schema catalogue writes admin-only. Improve `jazz-tools --help` and the docs so the CLI and publishing workflow more clearly explain when schema auto-push is enough versus when to run `permissions push` or `migrations push`.
- a41135e: Self-hosted servers now clean up disconnected client state after a configurable TTL, while deferring cleanup for clients that still have unprocessed inbox entries.
- 8b16d59: Replace Fjall with RocksDB as the default persistent storage engine for server, Node.js client, and CLI.

  **BREAKING:** Server data stored with Fjall is not compatible — existing servers must start from a clean data directory.

- b5193ad: Add World Tour Vue 3 example app demonstrating schema, permissions, live queries, file handling, and co-located component data access.
  - jazz-wasm@2.0.0-alpha.23
  - jazz-rn@2.0.0-alpha.23

## 2.0.0-alpha.22

### Patch Changes

- dedab8f: Add authorship-based edit metadata for row writes across the runtime and bindings.

  Rows now expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` magic columns in queries and permissions, and backend contexts can override stamped authorship with `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)`.

- 568aa27: Add reconcileArray for granular Svelte and Vue reactivity in onDelta callbacks
- 6aca383: Fix a sync-server permission bypass where replicated soft deletes could skip `DELETE` policy evaluation.

  User writes received as `ObjectUpdated` payloads now inspect delete metadata before the sync permission check is queued. Soft-delete commits are classified as `DELETE` operations instead of `UPDATE`, so replicated row deletions correctly use delete policies and are rejected when the client lacks delete access.

- 4d53497: Self-hosted server now supports JWKS key rotation without a restart. Keys are cached with a configurable TTL (5 minutes by default, override with `JAZZ_JWKS_CACHE_TTL_SECS`) and automatically refetched when a JWT arrives with an unknown key ID or a signature mismatch. A 10-second cooldown prevents forced refreshes from being abused as a DoS vector. If the JWKS endpoint goes down, the server continues validating against the stale cached keyset.
- fd7ecd0: Schema authoring no longer has a build/codegen step. Apps now define their schema directly in TypeScript with the namespaced API (`import { schema as s } from "jazz-tools"`), and `jazz-tools validate` is just an optional local preflight check.

  Current `permissions.ts` is now separate from the structural schema and migration lifecycle, instead of being versioned as part of schema identity.

  Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.

- 3bd07c5: Improve React Native runtime error reporting by normalizing UniFFI bridge failures into standard `Error` objects with stable `name`, `message`, `cause`, and `tag` metadata.

  Thanks [Schniz](https://github.com/Schniz)!

- 195db76: Add rune-based Svelte test infrastructure with real $state/$effect verification
- 113a73d: `jazz-tools server` now logs a ready-to-open inspector URL on startup using `https://jazz2-inspector.vercel.app/` with `url`, `appId`, and `adminSecret` encoded in the hash fragment.
- Updated dependencies [dedab8f]
- Updated dependencies [fd7ecd0]
  - jazz-wasm@2.0.0-alpha.22
  - jazz-rn@2.0.0-alpha.22

## 2.0.0-alpha.21

### Patch Changes

- 52b737b: Fix server-side row insert permission evaluation
- 65adab0: Add utils to simplify testing permissions
- eb31a76: Fix mixed `select("*", "$canDelete")` projections so permission introspection columns can be combined with wildcard row selection, including nested include projections, and document the supported query shape.
- 51094d9: Fix catalogue sync so clients receive shared catalogue updates correctly, and skip resending the catalogue on reconnect when the client and server are already aligned.
- 695862b: Allow TypeScript `update(...)` and `updateDurable(...)` calls to clear nullable fields with `null`.

  Passing `undefined` still leaves a field unchanged, and required fields still reject `null`.

- 47a9aae: Align the Vue and Svelte bindings more closely with React: Vue `useAll` now accepts `QueryOptions` and re-exports `DurabilityTier`/`QueryOptions`, while Svelte query subscriptions now use the shared subscription orchestrator, surface async subscription errors, and export `createExtensionJazzClient` and `attachDevTools` for extension tooling.
- 62406d3: Use separate fields for foreign key columns and resolved references
- Updated dependencies [477c43c]
  - jazz-rn@2.0.0-alpha.21
  - jazz-wasm@2.0.0-alpha.21

## 2.0.0-alpha.20

### Patch Changes

- 9f4d4d9: Bound oversized index keys by keeping as much real value prefix as fits in the durable key and appending a length plus hash overflow trailer.

  This keeps large indexed string and JSON equality lookups working without exceeding storage key limits, while preserving prefix-based ordering instead of collapsing oversized values to a pure hash ordering. Large `array(ref(...))` values also continue to support exact array equality and per-member reference indexing.

- Updated dependencies [9f4d4d9]
  - jazz-wasm@2.0.0-alpha.20
  - jazz-rn@2.0.0-alpha.20

## 2.0.0-alpha.19

### Patch Changes

- f2c10a1: Simplify `jazz-tools/backend` to expose only the high-level `Db` and `createJazzContext` APIs. `JazzContext` no longer exposes a low-level `client()` escape hatch, and the backend entrypoint no longer re-exports low-level runtime client and transport internals.
- 4015614: Add `.requireIncludes()` option to query builders to avoid loading rows if any of their included references are missing
- ecb2db8: Add high-level `Db` helpers for chunked browser file storage:
  `createFileFromBlob(...)`, `createFileFromStream(...)`, `loadFileAsBlob(...)`,
  and `loadFileAsStream(...)`.

  Document the conventional `files` / `file_parts` schema, permission setup, and
  blob/stream usage on the new Files & Blobs docs page.
  - jazz-wasm@2.0.0-alpha.19
  - jazz-rn@2.0.0-alpha.19

## 2.0.0-alpha.18

### Patch Changes

- ca2b0b0: Fix generated `schema/app.ts` query builder class fields for Expo compatibility by replacing `declare readonly` phantom fields with `readonly ...!:`.

  Expo's Babel pipeline rejects `declare` class fields without Flow-specific options, causing generated schemas to fail compilation in React Native apps. This change keeps the same type inference intent and does not change runtime behavior.

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

- d9261b7: Move Jazz client creation into the default React and React Native `JazzProvider` so Strict Mode remounts do not trigger extra startup delays, while still exposing `JazzClientProvider` for apps that need to supply their own client instance.

  Existing IndexedDB stores created by older builds are not checksum-compatible with this change and will need to be recreated after upgrading.

- Updated dependencies [33bc53f]
- Updated dependencies [83f4f5d]
  - jazz-wasm@2.0.0-alpha.18
  - jazz-rn@2.0.0-alpha.18

## 2.0.0-alpha.17

### Patch Changes

- 9002dde: Fix `jazz build` not regenerating `app.ts` on subsequent builds.

  The bin entry point was treating the TypeScript schema build as a one-time bootstrap step, skipping it whenever `current.sql` already existed. Removed the guard so `app.ts` and `current.sql` are always regenerated when `current.ts` is present.

- 6672f98: Remove local write-time foreign-key existence checks so inserts and updates no longer fail just because a referenced row has not been synced into the active query set yet.
- bb10f1c: Add a shared `jazz-tools/expo/polyfills` entrypoint for Expo apps and ensure published `jazz-rn` packages include the generated C++ bindings required for native builds.
  - jazz-wasm@2.0.0-alpha.17

## 2.0.0-alpha.16

### Patch Changes

- f81ecab: Fix backend N-API timestamp writes when `Timestamp` values arrive from TypeScript as JS numbers.

  `createJazzContext(...)` and other backend N-API mutation paths now accept integral epoch-millisecond timestamp payloads produced by the TS value converter, instead of rejecting modern dates as floating-point values during Rust deserialization.

- 30d2f08: Batch server-bound `/sync` payloads created in the same microtask into a single ordered request.
  - jazz-wasm@2.0.0-alpha.16

## 2.0.0-alpha.15

### Patch Changes

- 5684a18: Normalize schema manager table columns before hashing sorting by name.

  This makes logically equivalent schemas produce the same schema hash even when their column declarations are ordered differently.

- 6664ee5: Use the derived local anonymous/demo session for `JazzClient` query and subscription permission checks when no JWT is configured.
- 8877b8b: Fix runtime schema-order compatibility after sorted table columns.

  `Db` mutations and query transforms now tolerate runtime schemas returned as `Map`s, and low-level `JazzClient` create/query/subscribe APIs preserve the declared schema column order expected by generated bindings and app code.

- ac3a73e: Fix Rust schema-order compatibility when runtime table columns are sorted differently from the declared app schema, including `JazzClient` create/query flows and `SchemaManager` inserts.
- f9812d7: Fix lens SQL parsing for `TIMESTAMP` defaults so numeric defaults like `DEFAULT 0` are coerced to timestamp values instead of integers.

  This resolves type mismatches when applying migrations that add timestamp columns with numeric defaults, and adds regression coverage for `TIMESTAMP DEFAULT 0`.

- 4871b02: Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

  Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.

- 4fff7e9: Improve type inference for `include` and `select` in TS queries
- e32e6a9: Fix backend N-API sync regression where outbound messages were dropped before they reached the server.

  `createJazzContext(...).asBackend()` now accepts the real nested N-API sync callback shape used by published alpha builds, so backend query subscriptions and other upstream sync traffic can leave the local runtime again.

- 971f8cf: Add `$canRead`, `$canEdit`, and `$canDelete` permission introspection magic columns to queries, and reserve the `$` column prefix for system magic fields.
- bb39e15: Modify inserts to return the inserted row instead of just the id
- 8571fdb: Make query optional in `useAll` to support conditionally running queries when inputs are missing
- 9accce0: `QuerySubscription` in the Svelte bindings now accepts an options object as its second argument (e.g. `{ tier: 'edge' }`), matching the React `useAll` API. The previous bare-string form is removed.
- 78e074f: Split the local-first insert APIs in `jazz-tools`.
  - `db.insert(...)` now applies the write immediately and returns the inserted row synchronously.
  - `db.insertDurable(...)` waits for the requested durability tier before resolving.

- 4fd041c: Split the local-first update/delete APIs in `jazz-tools`.
  - `db.update(...)` and `db.delete(...)` now apply immediately and return `void`.
  - `db.updateDurable(...)` and `db.deleteDurable(...)` wait for the requested durability tier before resolving.
  - `db.deleteFrom(...)` has been renamed to `db.delete(...)`.

- Add Vue bindings and a `jazz-tools/vue` entrypoint, with matching docs and example coverage.
- Updated dependencies [bb39e15]
  - jazz-wasm@2.0.0-alpha.15

## 2.0.0-alpha.14

### Patch Changes

- ad29f43: fix query sync provenance for paginated, nested subquery, and recursive subscriptions
- a4da52d: Wait for the initial server event stream handshake before returning from `JazzClient::connect`, preventing `EdgeServer` settled queries from racing the connection after server restart.
- 78092d3: Add support for `EXISTS (SELECT FROM <table> WHERE <expr>)` in SQL policy expressions.
- 78092d3: Fix `@session.__jazz_outer_row.id` not resolving inside EXISTS subquery policies. Previously the outer row's UUID was silently treated as an unresolvable column, causing all EXISTS policy checks to evaluate to false on the server.
- dc25263: Fix: sync server now falls back to the server-established session when a `QuerySubscription` payload omits one.

  Demo and anonymous auth clients sent `session: None` in subscription payloads, causing all their queries to return empty results after the payload-session change in #147. The server now prefers the session it validated from auth headers during the SSE handshake, falling back to the payload only for fully unauthenticated clients. Payload sessions that differ from the server-established session are ignored and a warning is logged.

- 2943587: Fix a race condition in `subscribe_internal` where the callback could be called before it was registered.
- a952d98: Fix missing `id` fields on rows returned from included array subqueries, including nested relation results.
- ec0ff2d: Add a built-in MCP server (`npx jazz-tools mcp`) that exposes Jazz documentation as tools for AI assistants. Supports full-text search via SQLite FTS5 (Node 22.13+) with a plain-text fallback for older runtimes.
- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

- 49307fa: Quote keyword and non-bare identifiers when emitting frozen schema and lens SQL from Rust so round-tripping generated SQL continues to parse.
- Updated dependencies [2f5ccba]
  - jazz-wasm@2.0.0-alpha.14

## 2.0.0-alpha.13

### Patch Changes

- ff4ccb3: Support quoted SQL identifiers in `jazz-tools` schema parsing/generation, including reserved keyword column names like `"table"`.
  - jazz-wasm@2.0.0-alpha.13

## 2.0.0-alpha.12

### Patch Changes

- 8bcde79: Harden runtime sync outbox handling across WASM/RN and NAPI callback contracts by typing both callback shapes, routing both through a shared normalizer, and adding conformance tests that assert identical `/sync` behavior.
  - jazz-wasm@2.0.0-alpha.12

## 2.0.0-alpha.11

### Patch Changes

- 969a139: Overhauled durability APIs to use a single `DurabilityTier` model across reads and writes.
  - Reads now take `{ tier, localUpdates }`, where `localUpdates` defaults to `"immediate"` so local writes are reflected right away even when waiting for a more remote durability tier.
  - Writes now use the base methods with optional `{ tier }` and environment-aware defaults (`"worker"` for clients, `"edge"` for backend contexts).
  - Renamed the top tier from `"core"` to `"global"` for clearer semantics.
  - Added multi-tier node identity support so single-node deployments (like CLI and cloud-server today) can acknowledge both `"edge"` and `"global"`.

- 98ba0f9: Fixed array subquery incremental updates so parent row fields stay correct. Previously, when related rows changed after subscribing, update payloads could return corrupted parent values (for example, garbled `id` or `name`).
- 48053ac: fix(codegen): generate DROP COLUMN statements for all affected tables in multi-table migrations
- debd2c3: Add `asBackend()` for server-side Jazz clients using backend-secret auth, and enforce backend-role limits so backend sync can write row data but cannot write schema/permissions catalogue entries.
- a955504: Allow backend `JazzClient` and `SessionClient` query/subscribe calls to consume generated query builders directly. Query-builder payloads with `_schema` are now translated automatically to runtime query JSON (`relation_ir`), so backend code can call `context.forRequest(...).query(app.todos.where(...))` without manual `translateQuery(...)`.
  - jazz-wasm@2.0.0-alpha.11

## 2.0.0-alpha.10

### Patch Changes

- b058893: fix `jazz-tools build` bootstrap behavior by routing through the TypeScript schema CLI when `schema/current.ts` exists and `schema/current.sql` is missing
- ddf7756: Tighten generated query helper and include types for stronger inference and stricter contracts.

  This preserves include-aware returned row types by keeping `QueryBuilder<...WithIncludes<I>>` / `_rowType` aligned with selected includes, narrows generated `*Include` relation flags to `true` (instead of `boolean`), tightens `gather(...)` step callback typing, avoids optional-include selector collapse to `never` in nested array includes, and removes unnecessary `unknown` casts in generated include helpers.
  - jazz-wasm@2.0.0-alpha.10

## 2.0.0-alpha.9

### Patch Changes

- eef9942: Fix WebAssembly fetch behavior in Next.js runtimes.
  - jazz-wasm@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- 401db01: fix cold load of object history
- d1f17a9: fix: ensure query subgraphs share branch and schema context of parent graph
- 4775a79: Add a high-level server-side `createJazzContext` API in `jazz-tools/backend` with lazy runtime setup from generated app DSL objects, plus request/session-scoped helpers (`forRequest`, `forSession`) and lifecycle helpers (`flush`, `shutdown`).
  - jazz-wasm@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- Add Expo support.
- 6b19ea3: Add support for JSON columns.
- 47dbdba: Added Svelte support.
  - jazz-wasm@2.0.0-alpha.7
