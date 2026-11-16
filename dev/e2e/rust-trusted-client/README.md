# Rust trusted backend client against TS createJazz (local e2e, WIP)

Needs jazz-app from [jazz#5798](https://github.com/garden-co/jazz/pull/5798) (`Config::credential`,
`for_user`, `close`). Build this crate in a checkout of `feat/jazz-app-backend-client`. TS
`createJazz` can come from main.

The vitest file is `packages/jazz-tools/src/backend/rust-trusted-client.e2e-local.integration.test.ts`.
It sets up `trustWorkload({ …, claims: { sub: "ci" }, as: "backend" })` and
`createAccountFor("alice")`, and runs the binary with `SERVER_URL`, `APP_ID`, `TOKEN` and
`ALICE_ACCOUNT`. Setup and run are as in `../rust-scoped-client/README.md`, with
`RUST_CLIENT=…/trusted-client`.

Env switches: `SKIP_ALICE`, `SKIP_FOREIGN`, and `WAIT_FIRST` (sleep 3 s before `close`).
Without `WAIT_FIRST`, `close(CLOSE_TIMEOUT)` returns within about 1 ms with `Unsynced { pending: 1 }`
after a `for_user` write, so the test's `CLOSED ok` assertion fails. With `WAIT_FIRST=1` it passes.
