# SQLite browser reload qualification

The SQLite E2EE quickstart must restore the same account and committed messages
when both participating browser profiles reload. The permanent quickstart suite
covers eight concurrent reload cycles, in addition to creation, sharing, live
replies and nonmember isolation. It does not drain the runtimes before reload.

The root browser-test tooling uses Playwright 1.64.1 and its WebKit 27.2 build
(revision 2370 on Linux). Packages still pinned to Playwright 1.58.2 retain their
existing Chromium diagnostic patch. The newer driver already contains that fix.
No application storage, permission, sync or wire behaviour changes here.

## Reproduce

Build the current correctness WASM artifact and install the browser selected by
`JAZZ_BROWSER`, then run the normal quickstart suite:

```sh
pnpm --filter jazz-wasm build:fast
pnpm exec playwright install webkit
JAZZ_BROWSER=webkit pnpm --filter jazz-tools test:browser:sqlite-e2ee-quickstart
```

Use `chromium` or `firefox` for the other engines. The suite starts its SQLite
authority from source and uses the production SharedWorker and the actual React
quickstart App, schema and permissions. It checks independent browser contexts;
it does not qualify Safari on Apple hardware or persistence after a private
session closes.

## Validation on Linux

Both quickstart tests pass with Playwright 1.64.1 in Chromium 156.0.8078.4,
Firefox 157.0 and WebKit 27.2. Each engine passes the new eight-cycle concurrent
reload check with the same account IDs and messages. WebKit also passes the four
SQLite storage tests for physical ownership, persistence, reset and missing-file
refusal. The seven-test SQLite browser SDK suite passes in all three engines.

The new regression fails with the former WebKit 26.0 driver: one profile remains
at “Opening encrypted chat…” after a reload. The driver comparison uses the same
application and WASM sources. It does not add retries, extend per-operation
timeouts or alter existing test expectations.

## Older WebKit limitation

The former pin, Playwright 1.58.2, installs WebKit 26.0 (revision 2248 on Linux).
Repeated quickstart reloads intermittently leave a profile at “Opening encrypted
chat…”. External diagnostics narrowed one failure to the SQLite VFS preload:

- The worker answers probes and grants the physical storage lease.
- The catalogue cache loads successfully.
- Opening `jazz-idb` succeeds, but its `blocks` readonly transaction and bounded
  `getAll` request remain pending, with success and error handlers installed.
- Independent reads from the same page and a fresh tab also time out on that
  database. The other profile continues to read normally.

This resembles [WebKit bug 324094](https://bugs.webkit.org/show_bug.cgi?id=324094),
whose [fix landed in September 2026](https://github.com/WebKit/WebKit/pull/73930).
That report concerns transactions stranded when a worker ends. The exact cause
of the Jazz failure is not proven: separate small worker-termination tests did
not reproduce it. An unchanged quickstart with eight reload cycles passed using
an isolated Playwright 1.64.1/WebKit 27.2 installation before the repository pin
was changed.

A separate earlier WebKit 26.0 run trapped in a WASM callback during message
restoration. The later IndexedDB diagnosis does not establish the cause of that
trap. This tooling upgrade does not claim to repair older browsers or qualify
all Safari versions. Use current Chrome or Firefox for the preview walkthrough;
Apple-device and wider crash-schedule qualification remain outstanding.
