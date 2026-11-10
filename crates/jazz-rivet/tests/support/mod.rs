//! Deterministic page service, not a live WarpDrive cluster.
use async_trait::async_trait;
use rivet_envoy_protocol as p;
use sqlite_embedded::vfs::SqliteTransport;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};
#[derive(Default)]
pub struct Pages {
    data: Mutex<(u64, HashMap<u32, Vec<u8>>)>,
    // 1: refuse before commit; 2: commit but lose the acknowledgement.
    pub fail_next: AtomicU8,
    pause_commit: AtomicBool,
    pub commit_entered: tokio::sync::Notify,
    pub commit_release: tokio::sync::Notify,
}

impl Pages {
    pub fn pause_next_commit(self: &Arc<Self>) -> CommitPause {
        self.pause_commit.store(true, Ordering::SeqCst);
        CommitPause(self.clone())
    }
}

// Release even if a test assertion or timeout drops the fixture early.
pub struct CommitPause(Arc<Pages>);
impl Drop for CommitPause {
    fn drop(&mut self) {
        self.0.commit_release.notify_one();
    }
}

#[async_trait]
impl SqliteTransport for Pages {
    async fn get_pages(
        &self,
        req: p::SqliteGetPagesRequest,
    ) -> anyhow::Result<p::SqliteGetPagesResponse> {
        assert_eq!(req.expected_generation, Some(1));
        let state = self.data.lock().unwrap();
        Ok(p::SqliteGetPagesResponse::SqliteGetPagesOk(
            p::SqliteGetPagesOk {
                head_txid: Some(state.0),
                pages: req
                    .pgnos
                    .into_iter()
                    .map(|pgno| p::SqliteFetchedPage {
                        pgno,
                        bytes: state.1.get(&pgno).cloned().map(Into::into),
                    })
                    .collect(),
            },
        ))
    }
    async fn commit(&self, req: p::SqliteCommitRequest) -> anyhow::Result<p::SqliteCommitResponse> {
        assert_eq!(req.expected_generation, Some(1));
        if self.pause_commit.swap(false, Ordering::SeqCst) {
            self.commit_entered.notify_one();
            self.commit_release.notified().await;
        }
        let fail = self.fail_next.swap(0, Ordering::SeqCst);
        if fail == 1 {
            return Ok(p::SqliteCommitResponse::SqliteErrorResponse(
                p::SqliteErrorResponse {
                    group: "fixture".into(),
                    code: "refused".into(),
                    message: "commit refused".into(),
                },
            ));
        }
        let mut state = self.data.lock().unwrap();
        if let Some(expected) = req.expected_head_txid {
            assert_eq!(expected, state.0);
        }
        for page in req.dirty_pages {
            state.1.insert(page.pgno, page.bytes.to_vec());
        }
        state.1.retain(|pgno, _| *pgno <= req.db_size_pages);
        state.0 += 1;
        if fail == 2 {
            return Err(
                sqlite_embedded::vfs::SqliteCommitIndeterminateError("commit".into()).into(),
            );
        }
        Ok(p::SqliteCommitResponse::SqliteCommitOk(p::SqliteCommitOk {
            head_txid: Some(state.0),
        }))
    }
    async fn commit_stage_begin(
        &self,
        _: p::SqliteCommitStageBeginRequest,
    ) -> anyhow::Result<p::SqliteCommitStageBeginResponse> {
        anyhow::bail!("fixture does not implement staged commits")
    }
    async fn commit_stage_segment(
        &self,
        _: p::SqliteCommitStageSegmentRequest,
    ) -> anyhow::Result<p::SqliteCommitStageSegmentResponse> {
        anyhow::bail!("fixture does not implement staged commits")
    }
    async fn commit_finalize(
        &self,
        _: p::SqliteCommitFinalizeRequest,
    ) -> anyhow::Result<p::SqliteCommitFinalizeResponse> {
        anyhow::bail!("fixture does not implement staged commits")
    }
}
