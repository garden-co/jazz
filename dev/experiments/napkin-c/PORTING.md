# Napkin C port of the Jazz browser client: porting rules

Goal: a C translation of every Rust function that ships in the browser WASM
(`crates/jazz-wasm` and what it pulls in), compiled with clang to wasm32 at
`-Oz` + LTO + `wasm-opt`, so its size can be compared with the Rust build.
It only has to **compile** and be **roughly representative** of the shipped
logic. It never has to run. Size fidelity is the whole point, so:

- Translate logic **function by function**: same modules, same data structures,
  same algorithms, every match arm, every validation and error path, every
  error message string. Do not summarise a function into a stub. Do not pad.
- Write it the way a competent C programmer would write it by hand: one
  type-erased container implementation, vtables instead of generics, plain
  loops instead of iterator chains. That difference is what we are measuring.
- A "napkin" simplification is OK when it is size-neutral (e.g. a different but
  equally long way to express the same branch). Leaving out real logic is not.

## What to port (scope)

Port the non-test Rust code of your assigned files. Skip:

- `#[cfg(test)]` items, `tests/`, `benches/`, test-support/`testing` features,
  `cfg(not(target_arch = "wasm32"))` code, the `runtime` feature (tokio, jsonwebtoken),
  `cold-settle-attribution` / `sync-autopsy` / `bench-probes` features.
- Functions that LTO dropped from the shipped WASM. The list of every function
  in the shipped build (with its body size in bytes) is
  `/mnt/project-files/code-size-loop/browser-coverage.csv` (column `name`,
  `body_bytes`). Search it with `grep -F 'module::path::fn_name'`. If a
  non-trivial, non-generic, non-`#[inline]` function of yours has **no** row
  there and no caller of it does, it was dropped (or fully inlined into callers, which
  still counts as shipped). Rule of thumb: skip a function only if it is absent
  from the CSV **and** you cannot find a shipped caller; when in doubt, port it.
  `groove/src/queries.rs` and `groove/src/ivm/planner.rs` (SQL front-end) are known dropped.
- Note every skipped file/function family in your report.

Do not read files under /mnt/project-files other than the CSV and `generic-attribution.csv`
(both large; grep them, never cat them).

## Layout and naming

- Code lives in `dev/experiments/napkin-c/src/<crate>/<module path>.c` mirroring the
  Rust module tree (e.g. `crates/groove/src/ivm/runtime/join.rs` →
  `src/groove/ivm/runtime/join.c`). Headers next to them as `.h`.
- Use only `#include "jz_rt.h"` (in `rt/`) plus your **own unit's** headers.
  **Never include headers from another unit** (they are being written at the
  same time). For types and functions from other units, write local mirror
  declarations in `src/<crate>/_ext_<unit>.h` (struct layouts roughly like the Rust
  fields; opaque `typedef struct X X;` when you only pass pointers).
- **Function names** (non-static): the Rust path of the _defining_ module (file
  path, not the re-export path) with `::` → `__`, then type, then method:
  `groove__ivm__runtime__join__JoinState__apply_delta`, free fns
  `jazz_node__node__codec__decode_physical_mapping`. Crate prefixes:
  `groove`, `jazz_types`, `jazz_model`, `jazz_engine`, `jazz_protocol`,
  `jazz_node`, `jazz_db`, `jazz_wasm`, `idb_tree`, `jazz_compression`,
  `wasm_tracing`. Trait impls: `<impl module>__<Type>__<Trait>__<method>`.
  Use this exact scheme when you call into other units too, so the link resolves.
- Rust private (non-`pub`) fns used only in one file: `static`, but still use the full
  name scheme (the shipped Rust name section carries full paths too).
- Type names: `<crate>_<Type>` (e.g. `groove_Value`, `jazz_node_NodeState`).

## Translation conventions (runtime in rt/jz_rt.h)

- `Result<T, E>` → return `jz_err`, result via out-param. Error enums (thiserror)
  → a C enum of codes + `jz_err_fmt(code, "<same Display text>", ...)`. `?` → `JZ_TRY`.
- `Option<T>` → `bool has_x; T x;` or nullable pointer.
- Enums with data → tagged unions; `match` → `switch` with every arm.
- `Vec<T>` → `JZ_VEC(T)`; `String` → `jz_str`; `Vec<u8>`/`Bytes` → `jz_bytes`;
  `BTreeMap/BTreeSet` → `jz_btree` with a static `jz_btree_desc` per K/V kind (share descs);
  `HashMap/HashSet` → `jz_hmap`; `VecDeque` → `jz_deque`; `Rc/Arc` → `jz_rc_*`;
  `Intern<T>` → `jz_intern`; `Uuid` → `jz_uuid`; `serde_json::Value` → `jz_json`.
- `.sort*()` → `jz_sort`/`jz_sort_unstable` with a comparator fn; `collect::<BTreeMap>()` →
  insert loop or `jz_btree_from_pairs`.
- Traits used as generics (`S: Storage`, `impl Trait`) or `dyn Trait` → a vtable struct
  of function pointers + `void *self`. One C copy of each generic function.
- Closures → static helper fn + ctx struct, or inline the loop.
- Derives: `Drop` glue → write `<type>_drop()` for every owning type.
  `Clone`/`PartialEq`/`Ord`/`Hash` → write `_clone/_eq/_cmp/_hash` only where used
  (as map keys, `.clone()` calls, comparisons). `Debug` → `_debug(jz_str*, const T*)`
  only where the value is actually formatted (`{:?}` in messages/tracing).
  `Serialize`/`Deserialize` → hand-written postcard (`jz_pc_*`) and/or JSON
  (`jz_jw_*`, `jz_json_*`) encode/decode for every type that is actually serialized,
  following serde's format (variant index, field order, externally tagged JSON, etc.).
- `panic!/expect("msg")/unreachable!` → `jz_panic("msg")`; bare `.unwrap()` →
  `jz_unwrap_failed()`. No bounds checks on indexing (plain C).
- `async fn` → plain synchronous function; `.await` → direct call. Spawned
  futures → `jz_task_spawn`; channels → `jz_chan`; wakers → `jz_waker`.
  (Real suspension points such as IndexedDB requests become calls to imported host
  functions that are assumed to complete synchronously; we note this as an assumption.)
- `tracing::debug!` etc → `JZ_TRACE(level, "target", "fmt", ...)`; spans → `jz_trace_span_enter/exit`.
- `format!` → `jz_format`; `write!` into a String → `jz_str_appendf`.
- wasm-bindgen exports → `JZ_EXPORT("name")` functions taking `(ptr,len)` buffers /
  `jz_js` handles; JS calls → `JZ_IMPORT` functions.

## Compile check (required)

Every `.c` file you write must compile cleanly:

    dev/experiments/napkin-c/build/check.sh src/<crate>/path/to/file.c [more files...]

(it runs wasi-sdk clang `--target=wasm32-wasip1 -Oz -std=gnu11 -c` with
`-Werror=implicit-function-declaration -Werror=int-conversion -Werror=incompatible-pointer-types`).
Warnings other than those are fine. Linking is done later by the coordinator of the port.

## Report

When done, write `dev/experiments/napkin-c/reports/<unit>.md`: Rust files covered,
Rust non-test lines vs C lines per file, what you skipped and why (absent from CSV,
server-only, test-only), stubs you had to add, and anything notable about how the
Rust code mapped to C (e.g. "this generic was instantiated 12× in Rust, one copy in C").
