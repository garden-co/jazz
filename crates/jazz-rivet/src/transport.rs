//! Preserve Rivet's distinction between an unsuccessful and an ambiguous commit.
//! Based on rivetkit-core 3.0.7's EnvoySqliteTransport (Apache-2.0).
use anyhow::Result;
use async_trait::async_trait;
use rivet_envoy_client::{
    handle::EnvoyHandle,
    protocol as p,
    utils::{RemoteSqliteIndeterminateResultError, SqliteRequestTimeoutError},
};
use sqlite_embedded::vfs::{
    SqliteCommitIndeterminateError, SqliteStorageTimeoutError, SqliteTransport,
};

pub struct Transport(pub EnvoyHandle);
fn read_error(error: anyhow::Error) -> anyhow::Error {
    match error.downcast::<SqliteRequestTimeoutError>() {
        Ok(timeout) => SqliteStorageTimeoutError {
            operation: timeout.operation.to_owned(),
        }
        .into(),
        Err(error) => error,
    }
}
fn commit_error(error: anyhow::Error) -> anyhow::Error {
    let error = match error.downcast::<RemoteSqliteIndeterminateResultError>() {
        Ok(error) => return SqliteCommitIndeterminateError(error.operation.to_owned()).into(),
        Err(error) => error,
    };
    match error.downcast::<SqliteRequestTimeoutError>() {
        Ok(timeout)
            if timeout.sent && matches!(timeout.operation, "commit" | "commit_finalize") =>
        {
            SqliteCommitIndeterminateError(timeout.operation.to_owned()).into()
        }
        Ok(timeout) => SqliteStorageTimeoutError {
            operation: timeout.operation.to_owned(),
        }
        .into(),
        Err(error) => error,
    }
}
#[async_trait]
impl SqliteTransport for Transport {
    async fn get_pages(
        &self,
        request: p::SqliteGetPagesRequest,
    ) -> Result<p::SqliteGetPagesResponse> {
        self.0.sqlite_get_pages(request).await.map_err(read_error)
    }
    async fn commit(&self, request: p::SqliteCommitRequest) -> Result<p::SqliteCommitResponse> {
        self.0.sqlite_commit(request).await.map_err(commit_error)
    }
    async fn commit_stage_begin(
        &self,
        request: p::SqliteCommitStageBeginRequest,
    ) -> Result<p::SqliteCommitStageBeginResponse> {
        self.0
            .sqlite_commit_stage_begin(request)
            .await
            .map_err(commit_error)
    }
    async fn commit_stage_segment(
        &self,
        request: p::SqliteCommitStageSegmentRequest,
    ) -> Result<p::SqliteCommitStageSegmentResponse> {
        self.0
            .sqlite_commit_stage_segment(request)
            .await
            .map_err(commit_error)
    }
    async fn commit_finalize(
        &self,
        request: p::SqliteCommitFinalizeRequest,
    ) -> Result<p::SqliteCommitFinalizeResponse> {
        self.0
            .sqlite_commit_finalize(request)
            .await
            .map_err(commit_error)
    }
}
