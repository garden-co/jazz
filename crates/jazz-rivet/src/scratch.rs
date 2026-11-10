//! Isolated, bounded pages for Jazz's first-deployment validation. These pages
//! never go to WarpDrive and never share an actor's persistent namespace.
use async_trait::async_trait;
use rivet_envoy_protocol as p;
use sqlite_embedded::vfs::SqliteTransport;
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
pub(crate) struct Scratch(Mutex<(u64, HashMap<u32, Vec<u8>>)>);
#[async_trait]
impl SqliteTransport for Scratch {
    async fn get_pages(
        &self,
        req: p::SqliteGetPagesRequest,
    ) -> anyhow::Result<p::SqliteGetPagesResponse> {
        let state = self.0.lock().unwrap();
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
        let mut state = self.0.lock().unwrap();
        anyhow::ensure!(
            req.expected_head_txid.is_none_or(|head| head == state.0),
            "scratch head mismatch"
        );
        // Rivet's database page size is at most SQLite's 64 KiB maximum.
        // Bound by page number before allocating; this is only schema validation.
        anyhow::ensure!(
            req.db_size_pages <= 256,
            "Deployment validation exceeding 256 SQLite pages is not supported by this Rivet host"
        );
        for page in &req.dirty_pages {
            anyhow::ensure!(
                page.bytes.len() <= 65536 && page.pgno <= req.db_size_pages,
                "invalid scratch page"
            );
        }
        for page in req.dirty_pages {
            state.1.insert(page.pgno, page.bytes.to_vec());
        }
        state.1.retain(|pgno, _| *pgno <= req.db_size_pages);
        state.0 += 1;
        Ok(p::SqliteCommitResponse::SqliteCommitOk(p::SqliteCommitOk {
            head_txid: Some(state.0),
        }))
    }
    async fn commit_stage_begin(
        &self,
        _: p::SqliteCommitStageBeginRequest,
    ) -> anyhow::Result<p::SqliteCommitStageBeginResponse> {
        anyhow::bail!(
            "Staged commits for deployment validation are not supported by this Rivet host"
        )
    }
    async fn commit_stage_segment(
        &self,
        _: p::SqliteCommitStageSegmentRequest,
    ) -> anyhow::Result<p::SqliteCommitStageSegmentResponse> {
        anyhow::bail!(
            "Staged commits for deployment validation are not supported by this Rivet host"
        )
    }
    async fn commit_finalize(
        &self,
        _: p::SqliteCommitFinalizeRequest,
    ) -> anyhow::Result<p::SqliteCommitFinalizeResponse> {
        anyhow::bail!(
            "Staged commits for deployment validation are not supported by this Rivet host"
        )
    }
}
