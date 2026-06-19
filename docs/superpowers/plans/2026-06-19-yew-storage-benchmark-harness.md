# Yew Storage Benchmark Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the JavaScript comparison harness with a Yew/Trunk browser harness that uses two full Rust `gloo-worker` workers and still supports one headless CLI command.

**Architecture:** Add a benchmark-local Yew app under `crates/opfs-btree/wasm-bench/harness/`. The app spawns separate Rust workers for `opfs-btree` and SQLite, coordinates profile runs, verifies checksum parity, renders results, and exports a small automation result for a tiny Node launcher. The Node launcher only serves Trunk output, opens Chromium, waits, prints, and exits.

**Tech Stack:** Rust 2024, Yew, Trunk, `gloo-worker`, `gloo-net`, `wasm-bindgen`, `web-sys`, Playwright, existing `opfs-btree` wasm benchmark code, existing `bench-sqlite` Rust SQLite replay code.

---

## File Structure

- Create `crates/opfs-btree/wasm-bench/harness/Cargo.toml`: isolated Yew harness crate, not a workspace member unless Cargo forces it.
- Create `crates/opfs-btree/wasm-bench/harness/index.html`: Trunk entrypoint.
- Create `crates/opfs-btree/wasm-bench/harness/Trunk.toml`: public URL and dist settings.
- Create `crates/opfs-btree/wasm-bench/harness/src/main.rs`: Yew app entrypoint and UI.
- Create `crates/opfs-btree/wasm-bench/harness/src/app.rs`: state machine, profile scheduling, parity checks, automation export.
- Create `crates/opfs-btree/wasm-bench/harness/src/types.rs`: serializable shared message/result types.
- Create `crates/opfs-btree/wasm-bench/harness/src/fetch.rs`: shared async dataset fetching.
- Create `crates/opfs-btree/wasm-bench/harness/src/btree_worker.rs`: `gloo-worker` worker for `opfs-btree`.
- Create `crates/opfs-btree/wasm-bench/harness/src/sqlite_worker.rs`: `gloo-worker` worker for SQLite.
- Create `crates/opfs-btree/wasm-bench/harness/public/data/.gitignore`: ignore generated `.kv` and `.ops` assets copied for Trunk.
- Create `crates/opfs-btree/wasm-bench/prepare-harness-assets.cjs`: copy generated data into `harness/public/data`.
- Replace `crates/opfs-btree/wasm-bench/run-compare.cjs`: keep the filename as the tiny launcher so existing mental model and script name still work.
- Modify `crates/opfs-btree/package.json`: add Trunk/Yew harness scripts and repoint `bench:compare`.
- Modify `crates/opfs-btree/src/wasm_bench.rs`: expose typed Rust benchmark result structs and a Rust-returning dataset runner for worker use, while preserving wasm-bindgen exports used elsewhere.
- Modify `crates/opfs-btree/bench-sqlite/src/lib.rs`: expose typed Rust benchmark result structs and a Rust-returning dataset runner for worker use, while preserving the wasm-bindgen export if still useful.

## Task 1: Prove Two Rust Workers Build

**Files:**

- Create: `crates/opfs-btree/wasm-bench/harness/Cargo.toml`
- Create: `crates/opfs-btree/wasm-bench/harness/index.html`
- Create: `crates/opfs-btree/wasm-bench/harness/Trunk.toml`
- Create: `crates/opfs-btree/wasm-bench/harness/src/main.rs`
- Create: `crates/opfs-btree/wasm-bench/harness/src/types.rs`
- Create: `crates/opfs-btree/wasm-bench/harness/src/btree_worker.rs`
- Create: `crates/opfs-btree/wasm-bench/harness/src/sqlite_worker.rs`
- Modify: `crates/opfs-btree/package.json`

- [ ] **Step 1: Add the minimal harness crate**

Create `crates/opfs-btree/wasm-bench/harness/Cargo.toml`:

```toml
[workspace]

[package]
name = "opfs-btree-bench-harness"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
gloo-worker = "0.6"
serde = { version = "1", features = ["derive"] }
wasm-bindgen = "0.2"
yew = { version = "0.21", features = ["csr"] }
```

- [ ] **Step 2: Add the minimal Trunk files**

Create `crates/opfs-btree/wasm-bench/harness/index.html`:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>opfs-btree storage benchmark</title>
  </head>
  <body>
    <main id="app"></main>
  </body>
</html>
```

Create `crates/opfs-btree/wasm-bench/harness/Trunk.toml`:

```toml
[build]
target = "index.html"
dist = "dist"
public_url = "/"
```

- [ ] **Step 3: Add serializable smoke-test messages**

Create `crates/opfs-btree/wasm-bench/harness/src/types.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunProfile {
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerSmokeResult {
    pub engine: String,
    pub profile: String,
}
```

- [ ] **Step 4: Add the two minimal Rust workers**

Create `crates/opfs-btree/wasm-bench/harness/src/btree_worker.rs`:

```rust
use gloo_worker::{HandlerId, Worker, WorkerScope};

use crate::types::{RunProfile, WorkerSmokeResult};

pub struct BtreeWorker;

impl Worker for BtreeWorker {
    type Input = RunProfile;
    type Message = ();
    type Output = WorkerSmokeResult;

    fn create(_scope: &WorkerScope<Self>) -> Self {
        Self
    }

    fn update(&mut self, _scope: &WorkerScope<Self>, _msg: Self::Message) {}

    fn received(&mut self, scope: &WorkerScope<Self>, msg: Self::Input, id: HandlerId) {
        scope.respond(
            id,
            WorkerSmokeResult {
                engine: "opfs_btree".to_string(),
                profile: msg.profile,
            },
        );
    }
}
```

Create `crates/opfs-btree/wasm-bench/harness/src/sqlite_worker.rs`:

```rust
use gloo_worker::{HandlerId, Worker, WorkerScope};

use crate::types::{RunProfile, WorkerSmokeResult};

pub struct SqliteWorker;

impl Worker for SqliteWorker {
    type Input = RunProfile;
    type Message = ();
    type Output = WorkerSmokeResult;

    fn create(_scope: &WorkerScope<Self>) -> Self {
        Self
    }

    fn update(&mut self, _scope: &WorkerScope<Self>, _msg: Self::Message) {}

    fn received(&mut self, scope: &WorkerScope<Self>, msg: Self::Input, id: HandlerId) {
        scope.respond(
            id,
            WorkerSmokeResult {
                engine: "sqlite_inproc".to_string(),
                profile: msg.profile,
            },
        );
    }
}
```

- [ ] **Step 5: Add a minimal Yew app that spawns both workers**

Create `crates/opfs-btree/wasm-bench/harness/src/main.rs`:

```rust
mod btree_worker;
mod sqlite_worker;
mod types;

use btree_worker::BtreeWorker;
use gloo_worker::{Spawnable, WorkerBridge};
use sqlite_worker::SqliteWorker;
use types::{RunProfile, WorkerSmokeResult};
use yew::prelude::*;

enum Msg {
    RunSmoke,
    BtreeDone(WorkerSmokeResult),
    SqliteDone(WorkerSmokeResult),
}

struct App {
    btree: WorkerBridge<BtreeWorker>,
    sqlite: WorkerBridge<SqliteWorker>,
    rows: Vec<WorkerSmokeResult>,
}

impl Component for App {
    type Message = Msg;
    type Properties = ();

    fn create(ctx: &Context<Self>) -> Self {
        let btree = BtreeWorker::spawner()
            .spawn(ctx.link().callback(Msg::BtreeDone))
            .expect("spawn btree worker");
        let sqlite = SqliteWorker::spawner()
            .spawn(ctx.link().callback(Msg::SqliteDone))
            .expect("spawn sqlite worker");
        Self {
            btree,
            sqlite,
            rows: Vec::new(),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Msg::RunSmoke => {
                let req = RunProfile {
                    profile: "objects".to_string(),
                };
                self.btree.send(req.clone());
                self.sqlite.send(req);
                false
            }
            Msg::BtreeDone(result) | Msg::SqliteDone(result) => {
                self.rows.push(result);
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        html! {
            <>
                <h1>{"opfs-btree storage benchmark"}</h1>
                <button type="button" onclick={ctx.link().callback(|_| Msg::RunSmoke)}>{"Run"}</button>
                <ul>
                    { for self.rows.iter().map(|row| html! {
                        <li>{format!("{} {}", row.engine, row.profile)}</li>
                    }) }
                </ul>
            </>
        }
    }
}

fn main() {
    yew::Renderer::<App>::with_root(
        web_sys::window()
            .and_then(|window| window.document())
            .and_then(|document| document.get_element_by_id("app"))
            .expect("missing #app root"),
    )
    .render();
}
```

- [ ] **Step 6: Add a build script entry**

Modify `crates/opfs-btree/package.json` scripts:

```json
"bench:harness:build": "trunk build --config wasm-bench/harness/Trunk.toml wasm-bench/harness/index.html --release"
```

- [ ] **Step 7: Run the worker build proof**

Run:

```bash
pnpm --dir crates/opfs-btree run bench:harness:build
```

Expected: Trunk builds the Yew app and two Rust workers. If Trunk or `gloo-worker` requires a different registration shape, adjust only Rust/Trunk code. Do not add JavaScript worker bootstraps.

- [ ] **Step 8: Commit the build proof**

Run:

```bash
git add crates/opfs-btree/wasm-bench/harness crates/opfs-btree/package.json
git commit -m "bench: add Yew benchmark harness shell"
```

## Task 2: Extract Shared Typed Benchmark Results

**Files:**

- Modify: `crates/opfs-btree/src/wasm_bench.rs`
- Modify: `crates/opfs-btree/bench-sqlite/src/lib.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/src/types.rs`

- [ ] **Step 1: Make `opfs-btree` result types reusable**

In `crates/opfs-btree/src/wasm_bench.rs`, change the dataset replay result types from private to public and derive deserialize-compatible shapes only if needed by the harness:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetPhaseResult {
    pub phase: String,
    pub op_count: u32,
    pub elapsed_ms: f64,
    pub ops_per_sec: f64,
    pub checksum: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetRunResult {
    pub engine: String,
    pub profile: String,
    pub record_count: u32,
    pub phases: Vec<DatasetPhaseResult>,
    pub checksum: u64,
}
```

Update the existing `run_dataset` code to push `DatasetPhaseResult` instead of the old private `PhaseResult`.

- [ ] **Step 2: Add a Rust-returning `opfs-btree` runner**

In `crates/opfs-btree/src/wasm_bench.rs`, keep the existing wasm-bindgen export but make it call a public Rust function:

```rust
pub async fn run_dataset_result(
    kv_bytes: &[u8],
    ops_bytes: &[u8],
) -> Result<DatasetRunResult, JsValue> {
    run_dataset(kv_bytes, ops_bytes).await
}

#[wasm_bindgen]
pub async fn bench_dataset_run(kv_bytes: &[u8], ops_bytes: &[u8]) -> Result<JsValue, JsValue> {
    let out = run_dataset_result(kv_bytes, ops_bytes).await?;
    serde_wasm_bindgen::to_value(&out)
        .map_err(|e| JsValue::from_str(&format!("serialize dataset benchmark result: {e}")))
}
```

- [ ] **Step 3: Make SQLite result types reusable**

In `crates/opfs-btree/bench-sqlite/src/lib.rs`, rename the private result structs to public names:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetPhaseResult {
    pub phase: String,
    pub op_count: u32,
    pub elapsed_ms: f64,
    pub ops_per_sec: f64,
    pub checksum: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetRunResult {
    pub engine: String,
    pub profile: String,
    pub record_count: u32,
    pub phases: Vec<DatasetPhaseResult>,
    pub checksum: u64,
}
```

Update result construction to use `DatasetPhaseResult`.

- [ ] **Step 4: Add a Rust-returning SQLite runner**

In `crates/opfs-btree/bench-sqlite/src/lib.rs`, split the wasm-bindgen export:

```rust
pub async fn run_sqlite_dataset_result(
    kv: &[u8],
    ops: &[u8],
) -> Result<DatasetRunResult, JsValue> {
    let (profile, records) = decode_kv(kv);
    let phases = decode_ops(ops);
    let keys: Vec<&[u8]> = records.iter().map(|(k, _)| k.as_slice()).collect();
    let vals: Vec<&[u8]> = records.iter().map(|(_, v)| v.as_slice()).collect();
    let n = keys.len() as u32;

    log(&format!("[sqlite] {profile}: install sahpool + open ({n} records)"));
    install_opfs_sahpool::<ffi::WasmOsCallback>(&OpfsSAHPoolCfg::default(), true)
        .await
        .map_err(|e| JsValue::from_str(&format!("install sahpool: {e:?}")))?;

    let mut conn = open_conn()?;
    conn.execute_batch(
        "DROP TABLE IF EXISTS kv; CREATE TABLE kv(k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;",
    )
    .map_err(|e| JsValue::from_str(&format!("create: {e}")))?;

    let mut overall = n as u64;
    let mut phase_results = Vec::new();
    for phase in &phases {
        let started = now_ms();
        let (op_count, checksum, c) = replay_phase(conn, phase, &keys, &vals, n)?;
        conn = c;
        let elapsed = now_ms() - started;
        overall = overall.wrapping_add(checksum);
        phase_results.push(DatasetPhaseResult {
            phase: phase.name.clone(),
            op_count,
            elapsed_ms: elapsed,
            ops_per_sec: if elapsed > 0.0 {
                (op_count as f64) / (elapsed / 1000.0)
            } else {
                0.0
            },
            checksum,
        });
    }

    Ok(DatasetRunResult {
        engine: "sqlite_inproc".into(),
        profile,
        record_count: n,
        phases: phase_results,
        checksum: overall,
    })
}

#[wasm_bindgen]
pub async fn run_sqlite_dataset(kv: &[u8], ops: &[u8]) -> Result<JsValue, JsValue> {
    let out = run_sqlite_dataset_result(kv, ops).await?;
    serde_wasm_bindgen::to_value(&out).map_err(|e| JsValue::from_str(&e.to_string()))
}
```

- [ ] **Step 5: Mirror the public result shape in harness types**

Replace smoke result types in `crates/opfs-btree/wasm-bench/harness/src/types.rs` with:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunProfile {
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PhaseResult {
    pub phase: String,
    pub op_count: u32,
    pub elapsed_ms: f64,
    pub ops_per_sec: f64,
    pub checksum: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineRunResult {
    pub engine: String,
    pub profile: String,
    pub record_count: u32,
    pub phases: Vec<PhaseResult>,
    pub checksum: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerFailure {
    pub engine: String,
    pub profile: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResult {
    Ok(EngineRunResult),
    Err(WorkerFailure),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileComparison {
    pub profile: String,
    pub btree: EngineRunResult,
    pub sqlite: EngineRunResult,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkFailure {
    pub profile: Option<String>,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutomationResult {
    pub ok: bool,
    pub results: Vec<ProfileComparison>,
    pub error: Option<BenchmarkFailure>,
}
```

- [ ] **Step 6: Run targeted Rust checks**

Run:

```bash
cargo check -p opfs-btree --target wasm32-unknown-unknown
LLVM_PREFIX=""
for prefix in "$(brew --prefix llvm 2>/dev/null)" "$(brew --prefix llvm@20 2>/dev/null)"; do
  if [ -x "$prefix/bin/clang" ] && "$prefix/bin/clang" --print-targets | grep -qi wasm32; then
    LLVM_PREFIX="$prefix"
    break
  fi
done
test -n "$LLVM_PREFIX" || { echo "install Homebrew LLVM: brew install llvm"; exit 1; }

CC_wasm32_unknown_unknown="$LLVM_PREFIX/bin/clang" \
AR_wasm32_unknown_unknown="$LLVM_PREFIX/bin/llvm-ar" \
CFLAGS_wasm32_unknown_unknown="-O3 -DSQLITE_THREADSAFE=0" \
cargo check --manifest-path crates/opfs-btree/bench-sqlite/Cargo.toml --target wasm32-unknown-unknown
```

Expected: both pass. If SQLite fails because the wasm C toolchain is missing, install/configure the required LLVM clang before continuing; do not alter benchmark logic to bypass SQLite.

- [ ] **Step 7: Commit typed result extraction**

Run:

```bash
git add crates/opfs-btree/src/wasm_bench.rs crates/opfs-btree/bench-sqlite/src/lib.rs crates/opfs-btree/wasm-bench/harness/src/types.rs
git commit -m "bench: expose typed storage benchmark results"
```

## Task 3: Implement Dataset Fetching And Real Workers

**Files:**

- Create: `crates/opfs-btree/wasm-bench/harness/src/fetch.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/Cargo.toml`
- Modify: `crates/opfs-btree/wasm-bench/harness/src/main.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/src/btree_worker.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/src/sqlite_worker.rs`

- [ ] **Step 1: Add dependencies needed by real workers**

Modify `crates/opfs-btree/wasm-bench/harness/Cargo.toml` dependencies:

```toml
[dependencies]
gloo-net = "0.6"
gloo-worker = "0.6"
opfs-btree = { path = "../.." }
opfs-btree-bench-sqlite = { path = "../../bench-sqlite" }
serde = { version = "1", features = ["derive"] }
wasm-bindgen = "0.2"
wasm-bindgen-futures = "0.4"
web-sys = { version = "0.3", features = ["UrlSearchParams", "Window", "Location", "console"] }
yew = { version = "0.21", features = ["csr"] }
```

- [ ] **Step 2: Add shared dataset fetch helper**

Create `crates/opfs-btree/wasm-bench/harness/src/fetch.rs`:

```rust
use gloo_net::http::Request;

pub async fn fetch_dataset(profile: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let kv = fetch_bytes(&format!("/data/{profile}.kv")).await?;
    let ops = fetch_bytes(&format!("/data/{profile}.ops")).await?;
    Ok((kv, ops))
}

async fn fetch_bytes(path: &str) -> Result<Vec<u8>, String> {
    let response = Request::get(path)
        .send()
        .await
        .map_err(|e| format!("fetch {path}: {e}"))?;
    if !response.ok() {
        return Err(format!("fetch {path}: HTTP {}", response.status()));
    }
    response
        .binary()
        .await
        .map_err(|e| format!("read {path}: {e}"))
}
```

- [ ] **Step 3: Convert `BtreeWorker` to a real async worker**

Replace `crates/opfs-btree/wasm-bench/harness/src/btree_worker.rs` with:

```rust
use gloo_worker::{HandlerId, Worker, WorkerScope};
use wasm_bindgen_futures::spawn_local;

use crate::fetch::fetch_dataset;
use crate::types::{EngineRunResult, PhaseResult, RunProfile, WorkerFailure, WorkerResult};

pub struct BtreeWorker;

impl Worker for BtreeWorker {
    type Input = RunProfile;
    type Message = (HandlerId, RunProfile, Result<EngineRunResult, String>);
    type Output = WorkerResult;

    fn create(_scope: &WorkerScope<Self>) -> Self {
        Self
    }

    fn update(&mut self, scope: &WorkerScope<Self>, msg: Self::Message) {
        let (id, request, result) = msg;
        scope.respond(id, result_to_worker_output("opfs_btree", request.profile, result));
    }

    fn received(&mut self, scope: &WorkerScope<Self>, request: Self::Input, id: HandlerId) {
        let profile = request.profile.clone();
        let scope = scope.clone();
        spawn_local(async move {
            let result = async {
                let (kv, ops) = fetch_dataset(&profile).await?;
                let result = opfs_btree::wasm_bench::run_dataset_result(&kv, &ops)
                    .await
                    .map_err(js_error)?;
                Ok(convert_btree_result(result))
            }
            .await;
            scope.send_message((id, request, result));
        });
    }
}

fn js_error(value: wasm_bindgen::JsValue) -> String {
    value.as_string().unwrap_or_else(|| format!("{value:?}"))
}

fn result_to_worker_output(
    engine: &str,
    profile: String,
    result: Result<EngineRunResult, String>,
) -> WorkerResult {
    match result {
        Ok(result) => WorkerResult::Ok(result),
        Err(error) => WorkerResult::Err(WorkerFailure {
            engine: engine.to_string(),
            profile,
            error,
        }),
    }
}

fn convert_btree_result(result: opfs_btree::wasm_bench::DatasetRunResult) -> EngineRunResult {
    EngineRunResult {
        engine: result.engine,
        profile: result.profile,
        record_count: result.record_count,
        checksum: result.checksum,
        phases: result
            .phases
            .into_iter()
            .map(|phase| PhaseResult {
                phase: phase.phase,
                op_count: phase.op_count,
                elapsed_ms: phase.elapsed_ms,
                ops_per_sec: phase.ops_per_sec,
                checksum: phase.checksum,
            })
            .collect(),
    }
}
```

- [ ] **Step 4: Convert `SqliteWorker` to a real async worker**

Replace `crates/opfs-btree/wasm-bench/harness/src/sqlite_worker.rs` with:

```rust
use gloo_worker::{HandlerId, Worker, WorkerScope};
use wasm_bindgen_futures::spawn_local;

use crate::fetch::fetch_dataset;
use crate::types::{EngineRunResult, PhaseResult, RunProfile, WorkerFailure, WorkerResult};

pub struct SqliteWorker;

impl Worker for SqliteWorker {
    type Input = RunProfile;
    type Message = (HandlerId, RunProfile, Result<EngineRunResult, String>);
    type Output = WorkerResult;

    fn create(_scope: &WorkerScope<Self>) -> Self {
        Self
    }

    fn update(&mut self, scope: &WorkerScope<Self>, msg: Self::Message) {
        let (id, request, result) = msg;
        scope.respond(id, result_to_worker_output("sqlite_inproc", request.profile, result));
    }

    fn received(&mut self, scope: &WorkerScope<Self>, request: Self::Input, id: HandlerId) {
        let profile = request.profile.clone();
        let scope = scope.clone();
        spawn_local(async move {
            let result = async {
                let (kv, ops) = fetch_dataset(&profile).await?;
                let result = bench_sqlite::run_sqlite_dataset_result(&kv, &ops)
                    .await
                    .map_err(js_error)?;
                Ok(convert_sqlite_result(result))
            }
            .await;
            scope.send_message((id, request, result));
        });
    }
}

fn js_error(value: wasm_bindgen::JsValue) -> String {
    value.as_string().unwrap_or_else(|| format!("{value:?}"))
}

fn result_to_worker_output(
    engine: &str,
    profile: String,
    result: Result<EngineRunResult, String>,
) -> WorkerResult {
    match result {
        Ok(result) => WorkerResult::Ok(result),
        Err(error) => WorkerResult::Err(WorkerFailure {
            engine: engine.to_string(),
            profile,
            error,
        }),
    }
}

fn convert_sqlite_result(result: bench_sqlite::DatasetRunResult) -> EngineRunResult {
    EngineRunResult {
        engine: result.engine,
        profile: result.profile,
        record_count: result.record_count,
        checksum: result.checksum,
        phases: result
            .phases
            .into_iter()
            .map(|phase| PhaseResult {
                phase: phase.phase,
                op_count: phase.op_count,
                elapsed_ms: phase.elapsed_ms,
                ops_per_sec: phase.ops_per_sec,
                checksum: phase.checksum,
            })
            .collect(),
    }
}
```

- [ ] **Step 5: Import the fetch module**

Modify `crates/opfs-btree/wasm-bench/harness/src/main.rs` top-level modules:

```rust
mod btree_worker;
mod fetch;
mod sqlite_worker;
mod types;
```

- [ ] **Step 6: Run checks**

Run:

```bash
cargo check --manifest-path crates/opfs-btree/wasm-bench/harness/Cargo.toml --target wasm32-unknown-unknown
pnpm --dir crates/opfs-btree run bench:harness:build
```

Expected: both pass. Any failure that indicates `gloo-worker` cannot build the two Rust workers without JavaScript bootstraps is a design blocker and must be surfaced.

- [ ] **Step 7: Commit real worker implementation**

Run:

```bash
git add crates/opfs-btree/wasm-bench/harness
git commit -m "bench: run storage engines from Rust workers"
```

## Task 4: Implement Yew Coordination, UI, And Automation Export

**Files:**

- Create: `crates/opfs-btree/wasm-bench/harness/src/app.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/src/main.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/src/types.rs`
- Modify: `crates/opfs-btree/wasm-bench/harness/Cargo.toml`

- [ ] **Step 1: Add automation export helpers**

Add dependencies to `crates/opfs-btree/wasm-bench/harness/Cargo.toml`:

```toml
serde-wasm-bindgen = "0.6"
js-sys = "0.3"
```

- [ ] **Step 2: Add app state and coordination**

Create `crates/opfs-btree/wasm-bench/harness/src/app.rs`:

```rust
use std::collections::{BTreeMap, VecDeque};

use crate::btree_worker::BtreeWorker;
use crate::sqlite_worker::SqliteWorker;
use crate::types::{
    AutomationResult, BenchmarkFailure, EngineRunResult, ProfileComparison, RunProfile,
    WorkerResult,
};
use gloo_worker::{Spawnable, WorkerBridge};
use wasm_bindgen::JsValue;
use yew::prelude::*;

pub enum Msg {
    Run,
    BtreeDone(WorkerResult),
    SqliteDone(WorkerResult),
}

#[derive(Default)]
struct PendingProfile {
    btree: Option<EngineRunResult>,
    sqlite: Option<EngineRunResult>,
}

pub struct App {
    btree: WorkerBridge<BtreeWorker>,
    sqlite: WorkerBridge<SqliteWorker>,
    profiles: VecDeque<String>,
    pending: BTreeMap<String, PendingProfile>,
    results: Vec<ProfileComparison>,
    error: Option<BenchmarkFailure>,
    running: bool,
}

impl Component for App {
    type Message = Msg;
    type Properties = ();

    fn create(ctx: &Context<Self>) -> Self {
        let btree = BtreeWorker::spawner()
            .spawn(ctx.link().callback(Msg::BtreeDone))
            .expect("spawn btree worker");
        let sqlite = SqliteWorker::spawner()
            .spawn(ctx.link().callback(Msg::SqliteDone))
            .expect("spawn sqlite worker");
        let mut app = Self {
            btree,
            sqlite,
            profiles: profiles_from_query().into(),
            pending: BTreeMap::new(),
            results: Vec::new(),
            error: None,
            running: false,
        };
        if autorun_from_query() {
            ctx.link().send_message(Msg::Run);
        }
        app
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Msg::Run => {
                self.results.clear();
                self.error = None;
                self.running = true;
                self.dispatch_next_profile();
                true
            }
            Msg::BtreeDone(result) => {
                self.record_worker_result("opfs_btree", result);
                true
            }
            Msg::SqliteDone(result) => {
                self.record_worker_result("sqlite_inproc", result);
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        html! {
            <main>
                <h1>{"opfs-btree vs SQLite"}</h1>
                <button type="button" disabled={self.running} onclick={ctx.link().callback(|_| Msg::Run)}>
                    {"Run"}
                </button>
                { self.view_status() }
                { self.view_results() }
            </main>
        }
    }
}

impl App {
    fn dispatch_next_profile(&mut self) {
        if self.error.is_some() {
            self.finish();
            return;
        }
        let Some(profile) = self.profiles.pop_front() else {
            self.finish();
            return;
        };
        self.pending.insert(profile.clone(), PendingProfile::default());
        let request = RunProfile { profile };
        self.btree.send(request.clone());
        self.sqlite.send(request);
    }

    fn record_worker_result(&mut self, expected_engine: &str, result: WorkerResult) {
        if self.error.is_some() {
            return;
        }
        let result = match result {
            WorkerResult::Ok(result) => result,
            WorkerResult::Err(error) => {
                self.error = Some(BenchmarkFailure {
                    profile: Some(error.profile),
                    error: format!("{} worker failed: {}", error.engine, error.error),
                });
                self.finish();
                return;
            }
        };
        if result.engine != expected_engine {
            self.error = Some(BenchmarkFailure {
                profile: Some(result.profile),
                error: format!("expected {expected_engine}, got {}", result.engine),
            });
            self.finish();
            return;
        }
        let profile = result.profile.clone();
        let pending = self.pending.entry(profile.clone()).or_default();
        match expected_engine {
            "opfs_btree" => pending.btree = Some(result),
            "sqlite_inproc" => pending.sqlite = Some(result),
            other => {
                self.error = Some(BenchmarkFailure {
                    profile: Some(profile),
                    error: format!("unknown engine {other}"),
                });
                self.finish();
                return;
            }
        }
        if let Some(comparison) = self.take_complete_profile(&profile) {
            if comparison.btree.checksum != comparison.sqlite.checksum {
                self.error = Some(BenchmarkFailure {
                    profile: Some(profile),
                    error: format!(
                        "checksum mismatch: opfs-btree={} sqlite={}",
                        comparison.btree.checksum, comparison.sqlite.checksum
                    ),
                });
                self.finish();
                return;
            }
            self.results.push(comparison);
            self.dispatch_next_profile();
        }
    }

    fn take_complete_profile(&mut self, profile: &str) -> Option<ProfileComparison> {
        let pending = self.pending.get(profile)?;
        let btree = pending.btree.clone()?;
        let sqlite = pending.sqlite.clone()?;
        self.pending.remove(profile);
        Some(ProfileComparison {
            profile: profile.to_string(),
            btree,
            sqlite,
        })
    }

    fn finish(&mut self) {
        self.running = false;
        export_automation_result(AutomationResult {
            ok: self.error.is_none(),
            results: self.results.clone(),
            error: self.error.clone(),
        });
    }

    fn view_status(&self) -> Html {
        let text = if let Some(error) = &self.error {
            format!("Failed: {}", error.error)
        } else if self.running {
            "Running".to_string()
        } else {
            "Idle".to_string()
        };
        html! { <p>{text}</p> }
    }

    fn view_results(&self) -> Html {
        html! {
            <table>
                <thead>
                    <tr>
                        <th>{"Profile"}</th>
                        <th>{"Phase"}</th>
                        <th>{"opfs-btree ms"}</th>
                        <th>{"SQLite ms"}</th>
                        <th>{"opfs-btree ops/s"}</th>
                        <th>{"SQLite ops/s"}</th>
                    </tr>
                </thead>
                <tbody>
                    { for self.results.iter().flat_map(result_rows) }
                </tbody>
            </table>
        }
    }
}

fn result_rows(comparison: &ProfileComparison) -> Vec<Html> {
    comparison
        .btree
        .phases
        .iter()
        .map(|btree_phase| {
            let sqlite_phase = comparison
                .sqlite
                .phases
                .iter()
                .find(|phase| phase.phase == btree_phase.phase);
            html! {
                <tr>
                    <td>{comparison.profile.clone()}</td>
                    <td>{btree_phase.phase.clone()}</td>
                    <td>{format!("{:.2}", btree_phase.elapsed_ms)}</td>
                    <td>{sqlite_phase.map(|p| format!("{:.2}", p.elapsed_ms)).unwrap_or_default()}</td>
                    <td>{format!("{:.0}", btree_phase.ops_per_sec)}</td>
                    <td>{sqlite_phase.map(|p| format!("{:.0}", p.ops_per_sec)).unwrap_or_default()}</td>
                </tr>
            }
        })
        .collect()
}

fn profiles_from_query() -> Vec<String> {
    let Some(window) = web_sys::window() else {
        return vec!["objects".to_string(), "wikipedia".to_string()];
    };
    let search = window.location().search().unwrap_or_default();
    let params = web_sys::UrlSearchParams::new_with_str(&search).ok();
    params
        .and_then(|params| params.get("profiles"))
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|profile| !profile.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|profiles| !profiles.is_empty())
        .unwrap_or_else(|| vec!["objects".to_string(), "wikipedia".to_string()])
}

fn autorun_from_query() -> bool {
    let Some(window) = web_sys::window() else {
        return false;
    };
    let search = window.location().search().unwrap_or_default();
    web_sys::UrlSearchParams::new_with_str(&search)
        .ok()
        .and_then(|params| params.get("autorun"))
        .as_deref()
        == Some("1")
}

fn export_automation_result(result: AutomationResult) {
    let value = serde_wasm_bindgen::to_value(&result).unwrap_or(JsValue::NULL);
    if let Some(window) = web_sys::window() {
        let _ = js_sys::Reflect::set(&window, &JsValue::from_str("__benchDone"), &JsValue::TRUE);
        let _ = js_sys::Reflect::set(&window, &JsValue::from_str("__benchResult"), &value);
    }
}
```

- [ ] **Step 3: Make `main.rs` delegate to `App`**

Replace `crates/opfs-btree/wasm-bench/harness/src/main.rs` with:

```rust
mod app;
mod btree_worker;
mod fetch;
mod sqlite_worker;
mod types;

use app::App;

fn main() {
    yew::Renderer::<App>::with_root(
        web_sys::window()
            .and_then(|window| window.document())
            .and_then(|document| document.get_element_by_id("app"))
            .expect("missing #app root"),
    )
    .render();
}
```

- [ ] **Step 4: Run checks**

Run:

```bash
cargo check --manifest-path crates/opfs-btree/wasm-bench/harness/Cargo.toml --target wasm32-unknown-unknown
pnpm --dir crates/opfs-btree run bench:harness:build
```

Expected: both pass.

- [ ] **Step 5: Commit Yew coordination**

Run:

```bash
git add crates/opfs-btree/wasm-bench/harness
git commit -m "bench: coordinate storage benchmark in Yew"
```

## Task 5: Add Harness Assets And Tiny Launcher

**Files:**

- Create: `crates/opfs-btree/wasm-bench/harness/public/data/.gitignore`
- Create: `crates/opfs-btree/wasm-bench/prepare-harness-assets.cjs`
- Replace: `crates/opfs-btree/wasm-bench/run-compare.cjs`
- Modify: `crates/opfs-btree/package.json`
- Modify: `crates/opfs-btree/wasm-bench/README.md`

- [ ] **Step 1: Ignore generated harness data**

Create `crates/opfs-btree/wasm-bench/harness/public/data/.gitignore`:

```gitignore
*
!.gitignore
```

- [ ] **Step 2: Add asset preparation script**

Create `crates/opfs-btree/wasm-bench/prepare-harness-assets.cjs`:

```js
#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const here = __dirname;
const sourceData = path.join(here, "bench-data");
const targetData = path.join(here, "harness", "public", "data");

if (!fs.existsSync(sourceData)) {
  throw new Error(`missing generated bench data: ${sourceData}`);
}

fs.mkdirSync(targetData, { recursive: true });
for (const entry of fs.readdirSync(sourceData)) {
  if (!entry.endsWith(".kv") && !entry.endsWith(".ops")) continue;
  fs.copyFileSync(path.join(sourceData, entry), path.join(targetData, entry));
}

console.log(`copied benchmark data to ${targetData}`);
```

- [ ] **Step 3: Replace `run-compare.cjs` with the launcher**

Replace `crates/opfs-btree/wasm-bench/run-compare.cjs` with:

```js
#!/usr/bin/env node

const fs = require("fs");
const http = require("http");
const path = require("path");

const HERE = __dirname;
const DIST = path.join(HERE, "harness", "dist");

function loadPlaywright() {
  try {
    return require("playwright");
  } catch {
    return require(path.join(HERE, "..", "node_modules", "playwright"));
  }
}

function parseArgs(argv) {
  const out = { profiles: ["objects", "wikipedia"], json: false };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--profiles") {
      out.profiles = String(argv[++i] || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    } else if (argv[i] === "--json") {
      out.json = true;
    } else {
      throw new Error(`unknown argument: ${argv[i]}`);
    }
  }
  if (out.profiles.length === 0) throw new Error("--profiles needs at least one profile");
  return out;
}

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html";
  if (filePath.endsWith(".js")) return "text/javascript";
  if (filePath.endsWith(".wasm")) return "application/wasm";
  return "application/octet-stream";
}

function serveFile(res, filePath) {
  if (!filePath.startsWith(DIST) || !fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    res.writeHead(404);
    res.end("not found");
    return;
  }
  res.writeHead(200, { "Content-Type": contentType(filePath) });
  fs.createReadStream(filePath).pipe(res);
}

function printTable(result) {
  const rows = [];
  for (const comparison of result.results || []) {
    const sqliteByPhase = new Map(comparison.sqlite.phases.map((phase) => [phase.phase, phase]));
    for (const btreePhase of comparison.btree.phases) {
      const sqlitePhase = sqliteByPhase.get(btreePhase.phase);
      rows.push({
        profile: comparison.profile,
        phase: btreePhase.phase,
        btree_ms: Number(btreePhase.elapsed_ms).toFixed(2),
        sqlite_ms: sqlitePhase ? Number(sqlitePhase.elapsed_ms).toFixed(2) : "",
        btree_ops_s: Math.round(Number(btreePhase.ops_per_sec)),
        sqlite_ops_s: sqlitePhase ? Math.round(Number(sqlitePhase.ops_per_sec)) : "",
      });
    }
  }
  console.log("\n=== in-browser comparison: opfs-btree vs SQLite (Yew + Rust workers, OPFS) ===\n");
  console.table(rows);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const { chromium } = loadPlaywright();
  if (!fs.existsSync(path.join(DIST, "index.html"))) {
    throw new Error(`missing harness build output: ${DIST}`);
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1");
    const pathname = url.pathname === "/" ? "/index.html" : url.pathname;
    serveFile(res, path.join(DIST, decodeURIComponent(pathname)));
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    page.on("pageerror", (error) => console.error("[pageerror]", error.message));
    const profiles = encodeURIComponent(args.profiles.join(","));
    await page.goto(`http://127.0.0.1:${port}/?profiles=${profiles}&autorun=1`, {
      waitUntil: "load",
      timeout: 60000,
    });
    await page.waitForFunction(() => window.__benchDone === true, undefined, {
      timeout: 600000,
    });
    const result = await page.evaluate(() => window.__benchResult);
    if (!result || result.ok !== true) {
      throw new Error(result?.error?.error || "benchmark failed without an error payload");
    }
    if (args.json) {
      console.log(JSON.stringify(result, null, 2));
    } else {
      printTable(result);
    }
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
}

main().catch((error) => {
  console.error("compare failed:", error.message || error);
  process.exit(1);
});
```

- [ ] **Step 4: Update package scripts**

Modify `crates/opfs-btree/package.json` scripts to include:

```json
"bench:harness:assets": "node wasm-bench/prepare-harness-assets.cjs",
"bench:harness:build": "trunk build --config wasm-bench/harness/Trunk.toml wasm-bench/harness/index.html --release",
"bench:compare": "pnpm run bench:compare:data && pnpm run bench:harness:assets && pnpm run bench:harness:build && node wasm-bench/run-compare.cjs",
"bench:compare:open": "pnpm run bench:compare:data && pnpm run bench:harness:assets && trunk serve --config wasm-bench/harness/Trunk.toml wasm-bench/harness/index.html"
```

Leave `bench:sqlite:build` in place only if a downstream script still needs it. If the SQLite worker now compiles directly through the harness crate, remove it from `bench:compare`.

- [ ] **Step 5: Update README run instructions**

In `crates/opfs-btree/wasm-bench/README.md`, replace the old runner description with:

````markdown
The primary comparison runner is a Yew/Trunk harness. The harness spawns two
Rust `gloo-worker` workers, one for `opfs-btree` and one for SQLite. A small
Node launcher serves the built harness in headless Chromium and prints the
results for automation.

Run:

```bash
pnpm --dir crates/opfs-btree run bench:compare
```
````

For manual inspection:

```bash
pnpm --dir crates/opfs-btree run bench:compare:open
```

````

- [ ] **Step 6: Run launcher verification**

Run:

```bash
pnpm --dir crates/opfs-btree run bench:compare -- --profiles objects
````

Expected: headless Chromium runs the Yew harness, both Rust workers complete, checksums match, and the launcher prints a table.

- [ ] **Step 7: Commit launcher replacement**

Run:

```bash
git add crates/opfs-btree/package.json crates/opfs-btree/wasm-bench/prepare-harness-assets.cjs crates/opfs-btree/wasm-bench/run-compare.cjs crates/opfs-btree/wasm-bench/README.md crates/opfs-btree/wasm-bench/harness/public/data/.gitignore
git commit -m "bench: replace comparison runner with Yew harness"
```

## Task 6: Final Verification And Cleanup

**Files:**

- Review: `crates/opfs-btree/wasm-bench/harness/`
- Review: `crates/opfs-btree/wasm-bench/run-compare.cjs`
- Review: `crates/opfs-btree/package.json`
- Review: `crates/opfs-btree/wasm-bench/README.md`

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt --check
pnpm format:check
```

Expected: formatting passes. If `pnpm format:check` changes generated docs or unrelated files, stop and inspect before staging.

- [ ] **Step 2: Run targeted checks**

Run:

```bash
cargo check --manifest-path crates/opfs-btree/wasm-bench/harness/Cargo.toml --target wasm32-unknown-unknown
cargo check -p opfs-btree --target wasm32-unknown-unknown
LLVM_PREFIX=""
for prefix in "$(brew --prefix llvm 2>/dev/null)" "$(brew --prefix llvm@20 2>/dev/null)"; do
  if [ -x "$prefix/bin/clang" ] && "$prefix/bin/clang" --print-targets | grep -qi wasm32; then
    LLVM_PREFIX="$prefix"
    break
  fi
done
test -n "$LLVM_PREFIX" || { echo "install Homebrew LLVM: brew install llvm"; exit 1; }

CC_wasm32_unknown_unknown="$LLVM_PREFIX/bin/clang" \
AR_wasm32_unknown_unknown="$LLVM_PREFIX/bin/llvm-ar" \
CFLAGS_wasm32_unknown_unknown="-O3 -DSQLITE_THREADSAFE=0" \
cargo check --manifest-path crates/opfs-btree/bench-sqlite/Cargo.toml --target wasm32-unknown-unknown
```

Expected: all pass.

- [ ] **Step 3: Run full comparison**

Run:

```bash
pnpm --dir crates/opfs-btree run bench:compare
```

Expected: both default profiles complete, checksums match, and a result table prints.

- [ ] **Step 4: Confirm no generated harness assets are staged**

Run:

```bash
git status --short crates/opfs-btree/wasm-bench/harness/public/data crates/opfs-btree/wasm-bench/harness/dist
```

Expected: no generated `.kv`, `.ops`, or `dist` files are staged.

- [ ] **Step 5: Commit final cleanup if needed**

If Task 6 required any cleanup changes, run:

```bash
git add crates/opfs-btree/wasm-bench/harness crates/opfs-btree/wasm-bench/run-compare.cjs crates/opfs-btree/package.json crates/opfs-btree/wasm-bench/README.md
git commit -m "bench: verify Yew storage benchmark harness"
```

Skip this commit if there are no cleanup changes.

## Self-Review

- Spec coverage: The plan replaces `run-compare.cjs` as the main runner, adds a Yew app, uses two full Rust `gloo-worker` workers, preserves checksum parity, and keeps one CLI command.
- Placeholder scan: No `TBD`, `TODO`, or unspecified "handle later" steps are present. The only blocker path is explicit: stop if `gloo-worker` cannot satisfy the no-JavaScript-bootstrap requirement.
- Type consistency: `RunProfile`, `WorkerResult`, `EngineRunResult`, `ProfileComparison`, `BenchmarkFailure`, and `AutomationResult` are introduced in Task 2 and reused consistently in later tasks.
