# notes

A one-table app (`notes`, each read only by its creator) and a `createJazz`
backend for it (`server.mjs`), for `tests/end_user.rs`. `jazz.app.json` and
`jazz.permissions.json` are what `jazz-tools validate` writes for the app
`server.mjs` defines.

`tests/end_user.rs` starts `server.mjs` itself with the workspace's built
`jazz-tools`, so it needs that build (`pnpm build:correctness-artifacts`, then
build `jazz-tools`) and runs only with the `e2e-backend` feature:

```sh
cargo test -p jazz-app --features e2e-backend --test end_user
```

CI runs it in the TypeScript partition, after `jazz-tools` is built.
`JAZZ_APP_E2E_JAZZ_TOOLS=<dir>` points it at another `jazz-tools` package
(an installed one, say) when running locally.
