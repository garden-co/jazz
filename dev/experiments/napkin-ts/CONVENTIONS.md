# Napkin TypeScript port: conventions

Goal: estimate what the browser client would weigh if the Rust that ships in
the browser WASM had been written in TypeScript instead. The port must
**typecheck** and be **roughly representative of all the code**. It never runs.
Bundle size is the only output that matters, so the port should contain the
code a competent TypeScript team would have written for the same behavior:
no more (no padding, no comments-as-code), no less (no hollow stubs where Rust
has real logic).

## Layout

The Rust source tree lives at `/home/user/jazz` (origin/main). Map paths:

| Rust                                 | TypeScript                                                   |
| ------------------------------------ | ------------------------------------------------------------ |
| `crates/jazz/layers/<layer>/src/...` | `src/<layer>/...` (types, protocol, model, engine, node, db) |
| `crates/groove/src/...`              | `src/groove/...`                                             |
| `crates/jazz-compression/src/...`    | `src/compression/...`                                        |
| `crates/jazz-wasm/src/...`           | `src/wasm/...`                                               |
| `crates/idb-tree/src/...`            | `src/idb_tree/...`                                           |
| `crates/wasm-tracing/src/...`        | `src/wasm_tracing/...`                                       |

`foo.rs` → `foo.ts`, `foo/mod.rs` → `foo/index.ts`. Use `.js` in import specifiers
(`import { x } from "./foo.js"`). You may merge tiny files or split huge ones;
record that in your manifest.

## What to port

- Every non-test item: types, enums, structs, functions, methods, trait impls with real logic,
  async state machines. Skip `#[cfg(test)]` code, test-only helpers, `testing` feature code,
  benches, server-only `cfg(not(target_arch = "wasm32"))` paths, and `cold-settle-attribution` / `bench-probes` features.
- Keep algorithms, data structures, control flow and match arms. Port the logic, not the Rust ceremony:
  - derives (`Debug`, `Clone`, `PartialEq`, `Hash`, `Ord`, `Default`) → nothing, unless the code really
    needs structural equality/ordering/hashing for that type; then write one compare/key function.
  - `From`/`Into`/`AsRef`/newtype accessors → usually nothing or a one-liner.
  - Lifetimes, borrow juggling, `Arc<Mutex<..>>`, `Box::pin`, explicit drop → nothing.
  - Rust enums → discriminated unions (`{ kind: "Foo", ... }`) or classes; `match` → `switch`.
  - `Result<T, E>` → throw a typed error, or return a union where callers branch on it. Error enums → an error class
    (or a few) with the variant as `kind`.
- **Keep every error/format/log string verbatim.** They live in the WASM data section and count there too.
- Serialization: where Rust serializes with postcard / custom byte codecs (wire messages, storage rows, keys),
  write explicit `encodeX(w, x)` / `decodeX(r)` functions with `src/support/postcard.ts`. That is what serde derives
  generate in Rust. Where Rust only uses serde_json to talk to JS (`serde_wasm_bindgen`, JSON args), JS objects are
  already the representation: no code needed unless there is custom validation/conversion logic.
- Collections: `BTreeMap/BTreeSet` → `SortedMap/SortedSet` from `src/support/collections.ts` when order matters,
  else `Map`/`Set`. `HashMap` with struct keys → `KeyedMap` or a string key. `Vec` → arrays.
- Numbers: `u64`/`i64` → `number` unless the code depends on full 64-bit range or wrapping (hashes, packed keys) → `bigint`.
- Deps: use `src/support/*` (bytes, postcard, collections, hash/crypto/uuid via @noble, runtime helpers) and
  `fzstd` (zstd decode). No other npm packages; do not run `npm install`. Browser built-ins
  (`TextEncoder`, `crypto`, IndexedDB, `structuredClone`, `JSON`) are fine.
- Async: `async fn` → `async function`; futures channels → `Channel`/`deferred` in support/runtime.ts.

## Code owned by other partitions

You own only the Rust files listed for your partition. For anything else (other crates or other partitions of
your crate), **do not implement it**. Declare what you use in `src/_extern/<your-partition>.ts`:

```ts
import { extern } from "../support/runtime.js";
export type QueryPlan = any; // or a small interface with the fields you touch
export const lowerQuery = extern<(q: any, schema: any) => QueryPlan>();
export const RowId = extern<{ fromBytes(b: Uint8Array): any }>();
```

and import from there. Keep extern types loose (`any` or minimal interfaces); never put logic there.
Do not edit files outside your partition's TS paths and your own `_extern` file.

## Size of the result

Aim for what an idiomatic TS port of the same behavior would be. A typical TS line count is 50–80% of the Rust
non-test lines once ceremony is removed. Do not golf (minifier does that) and do not pad. Doc comments: optional,
keep them short (they are stripped anyway).

If time or context runs short, prefer porting every file at lower fidelity over porting half the files perfectly.
If you must leave something as a stub, mark it `// NAPKIN-STUB: <what is missing>` and record it in the manifest.

## Checks and reporting

- Typecheck: `cd /home/user/jazz/dev/experiments/napkin-ts && npx tsc --noEmit -p . 2>&1 | grep -E '^src/(<your dirs>|_extern/<your-partition>)'`
  must print nothing at the end. Other partitions are being written concurrently; ignore their errors.
  If tsc gets slow, you can check only your files with a temporary tsconfig that includes your dirs + `src/support` + your extern file.
- Manifest: write `manifests/<your-partition>.json`:
  `[{ "rust": "<path>", "rust_loc": N, "ts": ["src/..."], "status": "full" | "partial" | "stub", "note": "..." }]`
  where status says how much of the file's logic is really ported ("full" = all non-test logic at napkin fidelity).
- Do not commit, push, or call any `mcp__hearthbot__*` tools.
- Final report (your last message, under 300 words): TS files + line count, coverage (rust_loc full/partial/stub),
  and the 3–5 most interesting observations about where the Rust is bulky vs. its TS equivalent (e.g. "trait X impls
  are 400 Rust lines, 60 TS lines", "this codec is mostly derive-generated").
