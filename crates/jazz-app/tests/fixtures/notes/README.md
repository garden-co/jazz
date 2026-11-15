# notes

A one-table app (`notes`, read by their creator) and a `createJazz` backend
for it, for `tests/end_user.rs`. `jazz.app.json` and `jazz.permissions.json`
are what `jazz-tools validate` writes for `schema.ts` and `permissions.ts`.

`server.mjs`'s `authenticate` stands in for an app's own auth: it accepts
`Authorization: Bearer user:<id>`, or `user:<id>:<ttl>:<issued at>` for a
bearer that expires `ttl` seconds after `issued at` (Unix seconds).

```sh
cd crates/jazz-app/tests/fixtures/notes
npm init -y && npm pkg set type=module && npm i jazz-tools ws
node server.mjs   # prints {"appId": ..., "url": ...}
```

Then run the tests with that URL and app id (see `tests/end_user.rs`).
