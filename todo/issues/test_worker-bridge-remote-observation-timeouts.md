# Worker-bridge remote observation tests flake with 20s timeouts

## What

Two tests in `packages/jazz-tools/tests/browser/worker-bridge.test.ts` intermittently time out waiting for a remote browser Db to observe a baseline row before the test proceeds to block/disconnect the network:

- `Worker Bridge with OPFS > recovers sync after browser-side network loss with B in a separate context` (line 1033)
- `Worker Bridge with OPFS > promotes offline worker rows after reconnect while the worker stays alive` (line 1169)

Both fail with:

```
Remote browser db "test-sync-{recover,offline}-remote-…" did not observe title "baseline-…" within 20000ms; lastRows=[]; lastError=none
```

Originates from `waitForRemoteBrowserDbTitle` in `remote-db-harness.ts:141`. Reproduces both locally and in CI; is not a regression from the local-first-auth migration (seed-decode errors in the same file were fixed separately and those tests now pass).

## Priority

medium

## Notes

- Baseline row never arrives on the remote page — sync propagation to the second browser context appears to be the bottleneck, not auth or the feature under test.
- Bumping the 20s timeout would mask the issue rather than fix it; the underlying setup timing needs investigation.
- Other worker-bridge tests that also call `createSyncedDb` twice and then observe convergence (e.g. `propagates synced row from client A to client B`) pass consistently — so the regression is specifically in the "remote context" harness path, not general sync.
