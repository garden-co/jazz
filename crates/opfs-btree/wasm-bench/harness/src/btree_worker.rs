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
