# Rust backend clients against a TypeScript main backend

The Rust examples are **backend clients**: they sync through a TypeScript main backend's `/jazz` (`createJazz`) with a machine credential, as TypeScript's `createJazzClient` does. See [Backend Client](https://jazz.tools/docs/setup/backend-client).

This package runs each example's binary against a real main backend:

- `main-backend.ts`: a stock `createJazz` serving an example's own `jazz.app.json` and `jazz.permissions.json` (what `npx jazz-tools generate` writes from its `schema.ts`), with a stand-in OIDC issuer on loopback. It trusts one workload as `"backend"` (trusted) and another as the user `machine:rust-example` (scoped). Users `user-a` and `user-b` have accounts, so a trusted client can act for them.
- `tests/stage-plan-cli.test.ts`: [`stage-plan gate`](../stage-plan/apps/rust-cli) in both modes.
  - Scoped: a write, a read from a fresh client, and being kept out of another user's show.
  - Trusted: a system write and read, then `for_user` as A, and B kept out of A's show.
  - Refused tokens and the wrong kind of credential exit non-zero.
- `tests/backend-client.test.ts`: jazz-app-support's backend-client contract through [`probe/`](probe), a small binary, against a main backend on a local core server (`startMainBackend(dir, { core: "server" })`), whose admin API revokes an identity.
  - Trusted: `for_user` for a user with an account, `Error::NoAccount` for one without, `Error::Refused` (403 `identity_not_authorized`) for a revoked identity, then `close(CLOSE_TIMEOUT)`.
  - Scoped: `for_user` is refused. Untrusted: opening is refused.
- `tests/todo-servers.test.ts`: [`todo-server-rs`](../todo-server-rs) and [`docs/todo-server-rs`](../docs/todo-server-rs) as scoped clients. Writes made through their REST API reach the main backend, and the main backend's writes reach their API.

The tests build the binaries with `cargo build`, so they need a Rust toolchain.

```bash
pnpm --filter rust-backend-clients test
```

To try an example by hand, start a main backend and export what it prints:

```bash
pnpm --filter rust-backend-clients main-backend ../stage-plan/apps/rust-cli
```
