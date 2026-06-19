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
        scope.respond(
            id,
            result_to_worker_output("opfs_btree", request.profile, result),
        );
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
        checksum: result.checksum,
    }
}
