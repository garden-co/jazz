# Rust services and testing

## Authenticated service pattern

Keep one process-wide `JazzClient` in application state. For each request:

1. authenticate the bearer token or application session through the installed public auth API;
2. map the verified stable subject to a Jazz `Session`;
3. create `let db = client.for_session(session);`;
4. query through `db` so read policies filter rows;
5. mutate through `db` so write policies and authorship use the same caller;
6. await the required batch tier with a bounded timeout;
7. optionally requery through `db` at that tier before returning the representation.

Do not put the service's backend/admin secret in request authentication state. A backend credential
authorizes the service's transport role; the caller session determines row policy and authorship.

Inspect the installed JWT/JWKS middleware before wiring Axum. Some extractors are tied to the Jazz
server's own state type, so an application service may need a small app-specific extractor around
the public verification function rather than copying a server-only extractor.

Treat a policy-filtered row like an absent row when revealing its existence would leak data.

## Application error boundary

Map failures intentionally:

- missing or invalid caller auth: `401`;
- invalid path/body: `400` or `422`;
- absent or policy-filtered row: `404`;
- immediate or authoritative policy rejection: `403` or a domain-specific conflict;
- durability timeout/disconnection: `503` or `504`;
- positional decoder/schema mismatch: internal error with safe diagnostics.

The current broad `JazzError` variants can carry string details. Do not forward those strings
directly to untrusted clients.

## Repository testing rules

Read `crates/jazz-tools/TESTING_GUIDELINES.md` before writing tests in this repository. Its core rules
are:

- prefer black-box integration tests through public APIs;
- build schemas and permissions with `SchemaBuilder`, `TableSchema::builder`, `permissions(...)`,
  and policy expressions;
- use `row_input!` for inserts;
- use `JazzClient::test_client` for a sufficient one-runtime topology;
- otherwise start a `JazzServer` and connect independent clients;
- use `connect_with_row_policy_mode(..., RowPolicyMode::PermissiveLocal)` for untrusted-client
  simulation where the installed helper requires it;
- assert query rows, subscription deltas, accepted/rejected settlement, or visible row state;
- use `wait_for_query` and message blocking rather than internal polling or sleeps.

Inside this crate, reuse the established `TestingClient` support when available. External consumers
may need to construct public `AppContext`/`JazzClient` participants because repository-private test
helpers are not part of their crate API.

## Authorship and permission test

For an authenticated endpoint:

1. start a test JWT issuer and Jazz server with typed schema and policies;
2. connect the service client with its backend credential;
3. start the production router factory on an ephemeral listener or exercise it through Tower;
4. seed a row through one user and await `EdgeServer`;
5. call the real route as an allowed second user;
6. query through an independent client and select provenance columns;
7. assert the value is server-visible, creator is unchanged, and updater is the caller;
8. call as a denied user and assert the filtered/rejected behavior;
9. stop the listener, clients, issuer, and server.

Do not recreate route handlers in the test. Import and exercise the production router so the test
cannot drift from the service.

## Deterministic convergence test

Establish a common ancestor visible to both clients, then use the server's message-blocking support
or explicit disconnection so neither conflicting writer observes the other first. Run both delivery
orders, await batch settlement, and assert both clients plus a fresh third client converge. Load
`jazz-sync` for the expected merge semantics and current replay limitations.
