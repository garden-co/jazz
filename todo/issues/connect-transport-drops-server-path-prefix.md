# JazzClient.connectTransport drops serverPathPrefix

## What

`JazzClient.connectTransport(url, auth)` at `packages/jazz-tools/src/runtime/client.ts:1348-1353` calls `httpUrlToWs(url)` on the bare `serverUrl` and never appends the configured `serverPathPrefix`. Direct (non-worker) browser/RN clients therefore dial root `/ws` even when the Jazz server is mounted at `/apps/<id>` or similar. Worker init was fixed to thread the prefix through; the direct path regressed.

Call site in `db.ts:591-595` passes only `this.config.serverUrl` to `connectTransport`, so the prefix in `db.ts:560` (`serverPathPrefix: this.worker ? undefined : this.config.serverPathPrefix`) gets plumbed into `JazzClient.connectSync` but never reaches the transport-connect call.

The new test `packages/jazz-tools/src/runtime/db.transport.test.ts:40-58` (DBRT-U01) currently **locks in** the buggy shape: it passes `serverPathPrefix: "/apps/app"` but asserts `connectTransport` is called with `"https://example.test"` (no prefix). The test needs updating alongside the fix.

## Priority

high

## Notes

- Fix shape: either (a) change `connectTransport` to accept `serverPathPrefix` and compose the WS URL itself, or (b) compose the full URL in `db.ts` before calling `connectTransport`. (a) is more consistent with how the worker path is threaded.
- Deployments using a path prefix (reverse-proxy-mounted `/apps/<id>/ws`) will silently fail to sync upstream on the direct-client path — symptom looks like "edge tier never catches up".
- Unblocks deployments that rely on prefix routing to host multiple Jazz apps behind one origin.
