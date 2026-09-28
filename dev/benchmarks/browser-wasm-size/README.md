# Browser WASM size and runtime receipts

Build the real browser package with `pnpm --filter jazz-wasm build`. Preserve each
package generation in a separate ignored directory before building another arm.
Never replace another checkout's generated package or correctness artifact store.

## Byte attribution

```sh
node dev/benchmarks/browser-wasm-size/inspect.mjs \
  crates/jazz-wasm/pkg/jazz_wasm_bg.wasm target/wasm-size/bytes.json
```

Reports raw bytes, gzip level 9, Brotli quality 11, SHA-256, section sizes,
function body sizes, and totals by function-name prefix. These prefixes do not
assign generic functions to their calling crate or establish retained sizes.
Set `WASM_SIZE_SKIP_BROTLI=1` for quick iterations: quality 11 can take minutes.
The receipt still records gzip and explicitly leaves Brotli unavailable.

## Fresh Chromium comparison

Install the workspace dependencies and Playwright's Chromium first. For example:

```sh
node dev/benchmarks/browser-wasm-size/run.mjs \
  --artifact baseline=/absolute/baseline/pkg \
  --artifact candidate=/absolute/candidate/pkg \
  --order baseline,candidate,candidate,baseline \
  --rows 2000 --out target/wasm-size/browser.json
```

Every sample launches a new Chromium process. The runner serves the exact
provided WASM and glue, records their hashes, and checks producer manifests and
embedded fingerprints when present. A direct experimental `wasm-pack` build
without a producer manifest is recorded as such; this receipt does not qualify
it for production artifact consumers or replace the correctness-artifact gate.

The fixture builds its schema and queries with the public TypeScript builders.
For memory storage and real IndexedDB it measures database open, a transaction
inserting deterministic rows, the first full SELECT, the first filtered page,
and ten further full SELECTs. IndexedDB also closes and reopens the database,
then verifies persisted rows. Each query checks row counts, unique identities,
all application values, and the filtered page's membership. Receipts compare
the ordered row signatures across samples. No server or copied customer data
is required.

WASM compilation and instantiation have separate timers. Fetching the WASM
bytes happens before these timers; network transfer is not included. JS glue
import is measured separately over local HTTP. Query timers include the
TypeScript adapter's decoding. The repeated-read timer also includes the
independent value checks. Only the first memory read is first within a module;
the IndexedDB scenario follows the memory scenario in every sample.

Stop compiler jobs, compression jobs, and other CPU-heavy work before timing.
Alternate both directions and retain every sample. The script deliberately has
no wall-clock correctness threshold. Test additional query shapes before
claiming that a build profile has no general runtime cost.
