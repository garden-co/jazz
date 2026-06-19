use gloo_worker::{HandlerId, Worker, WorkerScope};

use crate::types::{EngineRunResult, RunProfile, WorkerResult};

pub struct BtreeWorker;

impl Worker for BtreeWorker {
    type Input = RunProfile;
    type Message = ();
    type Output = WorkerResult;

    fn create(_scope: &WorkerScope<Self>) -> Self {
        Self
    }

    fn update(&mut self, _scope: &WorkerScope<Self>, _msg: Self::Message) {}

    fn received(&mut self, scope: &WorkerScope<Self>, msg: Self::Input, id: HandlerId) {
        scope.respond(
            id,
            WorkerResult::Ok(EngineRunResult {
                engine: "opfs_btree".to_string(),
                profile: msg.profile,
                record_count: 0,
                phases: Vec::new(),
                checksum: 0,
            }),
        );
    }
}
