# SQLite private-owner WebSocket admission

An authenticated browser storage owner can now open the existing
`scope_isolated_client_relay` link to the SQLite server. Previously the server
refused every such connection with `UnsupportedFeature`, even after successful
session authentication and feature negotiation. That stopped the production
SharedWorker before encrypted application reads and writes could reach the
server.

## Admission and permissions

The runtime requires an ordinary authenticated session, the negotiated
`FEATURE_SCOPE_ISOLATED_CLIENT_RELAY` feature and a non-zero server-issued
admission epoch. SYSTEM, anonymous and trusted backend/authority identities
cannot use this runtime admission mode. The WebSocket route continues to
normalise an actual backend or admin credential to its ordinary trusted link;
requesting the browser capability does not change that credential's authority.

The accepted link runs as its immutable session identity with the claims supplied
by authentication. It does not become SYSTEM. Existing permission, account
revocation, resource-budget and shutdown checks remain on the normal route.

For example, Alice's two tabs can upload their own original `(node, transaction)`
pairs through Alice's owner. Each receives the authority's real receipt. Alice
cannot label an upload as Bob, replace her authenticated claims with Bob's, or
read as Bob. The backend-only scoped-fetch and accepted-overlay-check operations
remain refused on this connection, even when Alice requests her own identity.

Two tabs may use the same local transaction number. Their node identifiers keep
the receipts distinct. Retrying those exact uploads after a persistent authority
restart returns the stored authority result and creates no duplicate rows.
Connection-local echo hints are cleared on reconnect; all other receipt bytes,
including the original writer, transaction and authority cut, remain unchanged.

The server passes private-link admission into the SQLite subscriber. Before it
looks up a prior transaction result, it requires the upload to name the admitted
author exactly. A forged or missing author receives a rejection without replacing
the original transaction's recorded result. A valid retry can still settle once.
Ordinary workers retain upstream's connection-based attribution behaviour, and
trusted backend links retain their separate delegation contract.

Authentication supplies the private connection's permission claims. Payload
claims cannot replace them: an ordinary node's own upload can carry an empty
claims map while still being checked against its authenticated connection. This
keeps claim-dependent policies enforced without treating local metadata as a
credential.

## Protocol compatibility

This port changes admission behaviour, not wire or storage bytes. It uses the
existing `jazz-websocket-prelude-v1` requested-link field, feature bit, server Hello,
SQLite upload format and origin-qualified fate encoding. The shared prelude
fixture and all message discriminants are unchanged. The server issues a fresh
endpoint epoch on each accepted connection; it never takes an epoch supplied by
the browser as authentication.

An older SQLite server still refuses the private-owner link. Deploy the server
port before relying on this browser path. The rest of the E2EE stack retains its
own coordinated-upgrade requirements, including the newer scoped-evidence and
accepted-overlay messages. This admission change does not port the separate
stateless server-relay role or alter the accepted-read limits.

## Reproducible browser check

From the repository root, with dependencies installed:

```sh
pnpm --filter jazz-wasm build:fast
pnpm --filter jazz-tools test:browser:sqlite-e2ee
```

Select another installed Playwright engine with `JAZZ_BROWSER`:

```sh
pnpm exec playwright install firefox webkit
JAZZ_BROWSER=firefox pnpm --filter jazz-tools test:browser:sqlite-e2ee
JAZZ_BROWSER=webkit pnpm --filter jazz-tools test:browser:sqlite-e2ee
```

The dedicated suite verifies the current WASM producer manifest,
generates its SDK fingerprint expectation from that verified manifest, and
checks provenance again after the run. It builds the actual `jazz-tools` server
binary from this checkout and starts it on an ephemeral port. It bundles the
production SharedWorker into a private test directory. It neither loads NAPI nor
substitutes a test worker for the application worker. The ordinary dual-artifact
correctness runner is unchanged.

The suite has three encrypted-chat tests and four existing SQLite storage tests.
All seven pass in the installed Playwright Chromium, Firefox and WebKit builds
on Linux. They cover encrypted chat creation, global settlement, reopening and
decryption, sharing with a second account, a reply delivered through a live
subscription, and refusal to decrypt for an unshared third account.

The profile scenario opens Alice, Bob and Mallory in separate browser contexts
at the same origin, using identical database names. It checks independent account
IDs and localStorage, secure contexts and Web Crypto availability. Alice shares
with Bob; Bob replies and Alice receives the plaintext through her subscription.
Bob then closes his application page and opens a fresh JavaScript realm. His
account selection and encryption keys restore through the ordinary browser
stores. The harness never copies an account secret or encryption key between
pages or profiles. Mallory remains unable to decrypt before and after the reply.

The private context stays open during Bob's page replacement. WebKit keeps an
inert, opaque-origin anchor page for this purpose; it owns no application worker
or keys. Closing the entire private session can discard its storage. These tests
do not promise persistence beyond that session, qualify Safari on Apple devices,
or cover every crash schedule, encrypted query shape, React UI or mobile runtime.

The Rust tests cover runtime admission, direct session permissions, forged
attribution and claims, backend-only request rejection over WebSocket, and
receipt replay after a persistent authority restart. The two new WebSocket cases
fail against the previous implementation at its explicit unsupported-link
response.

The existing prelude corpus test still expects a negotiated private-owner link
to be unsupported. Its separately prepared expectation update awaits approval
under AGENTS.md and remains unapplied. The stack remains draft; the native binding
port and broader browser/application and mobile qualification are separate.
