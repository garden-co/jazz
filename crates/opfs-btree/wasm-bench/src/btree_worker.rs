use gloo_worker::{HandlerId, Worker, WorkerScope};
use wasm_bindgen_futures::spawn_local;

use crate::btree_engine::BtreeEngine;
use crate::clock::now_ms;
use crate::fetch::fetch_data;
use crate::types::{EngineRunResult, RunProfile, WorkerFailure, WorkerResult};

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
        scope.respond(
            id,
            result_to_worker_output("opfs_btree", request.profile, result),
        );
    }

    fn received(&mut self, scope: &WorkerScope<Self>, request: Self::Input, id: HandlerId) {
        let base_url = request.base_url.clone();
        let profile = request.profile.clone();
        let scope = scope.clone();
        spawn_local(async move {
            let result = run_btree_dataset(&base_url, &profile).await;
            scope.send_message((id, request, result));
        });
    }
}

async fn run_btree_dataset(base_url: &str, profile: &str) -> Result<EngineRunResult, String> {
    let benchmark = bench_core::benchmark(profile).ok_or_else(|| format!("unknown profile: {profile}"))?;
    let kv = fetch_data(base_url, &benchmark.kv_fixture).await?;
    let dataset = bench_core::decode_kv(&kv).map_err(|e| e.to_string())?;
    let mut engine = BtreeEngine::open().await.map_err(|e| e.to_string())?;
    let result = bench_core::run(&mut engine, "opfs_btree", &benchmark, &dataset, &now_ms)
        .await
        .map_err(|e| e.to_string())?;
    Ok(result.into())
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
