# Text editor browser benchmarks

These cases drive the actual React example in Chromium through Playwright. The
Vitest benchmark runner with the CodSpeed plugin measures three user flows at 10,000, 50,000, 100,000,
and 259,778 historical edits:

- **Open:** navigate in a fresh browser context until CodeMirror holds the expected
  text and is editable. Includes page initialization, Jazz delivery and Yjs replay.
- **Reload:** load once outside the timer, disconnect Jazz, block only the Jazz
  server's WebSocket, then time reloading the page from the persistent browser DB.
- **Sync an edit:** load the document in two separate browser contexts outside the
  timer, then insert one character and wait for the second editor to contain it.

The server runs in memory on localhost; the app uses Vite development mode, with
its inspector disabled. Browser launch, context creation, fixture generation,
seeding, full-text validation and teardown are outside each timed operation.
Playwright communication and waiting for the loaded view are included. Results
are browser user-flow timings, not native storage-only timings or WAN estimates.

Each trial verifies the full resulting editor text. Each edit trial starts from a
new seeded document so trials don't accumulate edits or session logs. The example
uses default Yjs garbage collection and retains all individually encoded updates
in Jazz. There is no log consolidation.

## Run

With current release WASM/NAPI artifacts and the compiled Jazz Tools runtime:

```sh
node dev/artifacts/verify-starter-e2e-artifacts.mjs
pnpm --filter text-editor exec playwright install chromium
pnpm --filter text-editor bench
```

For a quick check:

```sh
BENCH_ROUNDS=1 BENCH_FILTER='[10000]' pnpm --filter text-editor bench
```

Three rounds per case run by default. `BENCH_FILTER` selects names by substring.
The runner starts and stops its own Vite/Jazz server and browser, and writes benchmark results to `.generated/results.json`. The benchmark-only Vite transform exposes
an existing app database and a read-only editor accessor; it does not replace the
editor, provider, schema, permissions or initialization code.

The CodSpeed workflow builds release runtimes separately, verifies their artifact
fingerprints on the measurement host, then runs these cases on `codspeed-macro`.
The examples page reads the resulting release/main history through its existing
CodSpeed pipeline. Before the first hosted run, cards display no measurement;
local measurements are never inserted as hosted results.

## Fixture attribution

`fixture.mjs` downloads the Automerge paper editing trace by Martin Kleppmann and
Alastair R. Beresford from the checksum-pinned asset in
`crates/jazz-sim/fixtures/manifest.json`. The trace is from
[automerge/automerge-perf](https://github.com/automerge/automerge-perf), associated
with _A Conflict-Free Replicated JSON Datatype_, and licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
The conversion replays its edits into Yjs and concatenates the emitted updates
without changing the text. Generated files stay in the ignored `.generated/`
directory; the normal example still creates empty documents.
