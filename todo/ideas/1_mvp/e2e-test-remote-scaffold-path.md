# E2E test for remote scaffold path

## What

Add an integration test that drives `create-jazz` through its real production
path: `tiged` clone from GitHub + `resolveRemoteDeps` against raw.githubusercontent.
Today every scaffold test sets `JAZZ_STARTER_PATH` or relies on local fixtures,
so the code path that `npx create-jazz` actually runs has zero coverage.

Blocked until the repo is public — `tiged` and the raw fetches both need
unauthenticated GitHub access.

## Notes

Possible shapes:

- stub `globalThis.fetch` + mock `tiged` to serve canned starter + workspace YAML
- stand up a tiny git-serve fake over http
- run the real path against the public repo once the flip happens

Would naturally cover the crates/ fallback terminal failure and the
non-404 transient branch in `resolveRemoteDeps`.
