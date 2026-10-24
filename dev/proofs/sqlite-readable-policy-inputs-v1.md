# Readable permission inputs for private SQLite owners

A cold private owner can receive an authorised message without having the
membership rows needed to evaluate its read policy. That previously made the
React E2EE quickstart show an empty chat for a newly granted recipient.

Private owners now request readable permission inputs with their ordinary
subscriptions. The authority collects the indexed dependencies of the selected
rows and sends them through normal row, content and acceptance sync. Bob can open
Alice's shared chat without separately querying membership tables. Revoking his
grant removes the message from the live result. Replacing a grant refreshes the
inputs even when the message itself has not changed.

## Permission and resource boundaries

The authority checks every dependency's SELECT policy under the requesting
connection's identity and claims before publishing the closure. Two connections
of one account retain their own claim scopes, including during retained-row
updates. Requesting inputs grants no backend capability or alternative identity.

Collection follows indexed UUID or declared-reference correlations and the
bounded relation collector. One shared 16,384-unit work budget covers roots,
policy terms, fields, relation queries and the recursive dependency closure.
Unrelated tables are not loaded. The normal watch machinery tracks shipped
coordinates; keyed query invalidation refreshes inputs for unchanged results.

An unreadable dependency refuses the subscription with a typed error. It is
never sent as hidden evidence to an ordinary session. The refusal propagates
through the private owner, releases that subscription and leaves other reads and
writes usable. Ordinary authority subscriptions retain their existing behaviour;
applications whose policies depend on unreadable rows can still use that path.

Readable-input subscriptions currently require the current schema at the
connected authority. Other-schema requests, branched inputs and dependency
shapes outside the indexed collector's limits are refused explicitly. A generic
intermediate relay cannot collect this authority-only closure. Existing
trusted-backend scoped-evidence reads have their separate admission rules.

## Wire compatibility

`jazz.sqlite.sync.readable-policy-subscribe.v1` is appended as SQLite sync message
tag **40**, `SubscribeReadablePolicyV1`. Its ordered fields are:

1. `id: u64`
2. the existing serialized `Query`
3. `binding: BTreeMap<String, Value>`
4. `since: Option<u64>`
5. `include_deleted: bool`

Responses use existing row images, accepted-row metadata, content witnesses,
settlements and refusal messages. No stored table or existing message layout
changes. The fixture `crates/jazz/fixtures/sqlite_readable_policy_subscription_v1.json`
pins both the new request and the unchanged ordinary subscription, including a
multi-byte request ID, resume cut, binding and query window.

Upgrade the unreleased SQLite authority and private-owner runtimes together.
An older peer cannot decode the appended request. There is no fallback that
claims a complete private-owner result while omitting its required inputs.
Reconnects retain the request mode and reconcile authority revocations through
the existing held-row and resubscription protocol.

## Reproduction and validation

From the repository root:

```sh
cargo test -p jazz --test sqlite_owner_policy_inputs
pnpm --filter jazz-wasm build:fast
pnpm --filter jazz-tools typecheck:sqlite-e2ee
pnpm --filter jazz-tools test:browser:sqlite-e2ee-quickstart
JAZZ_BROWSER=firefox pnpm --filter jazz-tools test:browser:sqlite-e2ee-quickstart
JAZZ_BROWSER=webkit pnpm --filter jazz-tools test:browser:sqlite-e2ee-quickstart
```

The Rust regressions cover cold reads, revocation, grant replacement without a
message edit, reconnect, query-scoped collection, unreadable-input refusal,
connection-scoped claims and fixed wire bytes. The cold-read regression fails
against the preceding implementation.

The browser scenario renders the actual quickstart App, schema and permissions
inside its documented React provider and StrictMode. It uses independent Alice,
Bob and Mallory browser contexts, the production SharedWorker, verified WASM and
a source-built SQLite authority. Asset URLs and the authority address are
injected by the harness.

UI actions create the chat, send the first message, share it and send a live
reply. A small probe checks the nonmember's authoritative read and Bob's forbidden
grant, which the UI hides. Reloads check account identity and message restoration.
The test does not copy account secrets or encryption keys between profiles.

This qualifies the rendered application flow, not project scaffolding or the
Vite plugin's deployment steps. Playwright's Linux engines do not qualify Safari
on Apple devices or mobile runtimes. The private context remains open during
reload; persistence after closing the entire private session is not promised.
