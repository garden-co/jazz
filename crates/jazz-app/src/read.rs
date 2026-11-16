//! Read options, as TS's query options: the tier a read waits for, how long
//! a local-first read waits for the server's first answer, and which large
//! columns a subscription's updates carry as splices, and the branch view
//! a read goes through.

use std::time::Duration;

use jazz::db::{FirstLoad, ReadOpts};
use jazz::tools::ReadTier;

use crate::branch::BranchView;
use crate::error::{Error, Result};
use crate::handle::{DefaultRead, Jazz, decode_result, read_opts};
use crate::include::Row;
use crate::query::{Column, Query};
use crate::subscription::{self, Subscription};
use crate::value::ColumnValue;

/// How a read or subscription runs. `ReadOptions::default()` reads as a
/// read that names no tier does ([`crate::DefaultRead`]), as TypeScript's
/// options without a `tier`.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    tier: Option<ReadTier>,
    first_load_remote_wait: Option<Duration>,
    splices: Vec<String>,
    branch: Option<BranchView>,
}

impl ReadOptions {
    /// Reads at `tier` (TS's `tier`).
    pub fn tier(mut self, tier: ReadTier) -> Self {
        self.tier = Some(tier);
        self
    }

    /// Reads local data first, but waits up to `wait` for the server's
    /// answer before the first result (TS's `firstLoadRemoteWaitMs`).
    /// Offline, or once `wait` passed, it answers locally. Only for
    /// local-first reads: with [`ReadOptions::tier`] `Remote`, which waits for
    /// the server anyway, the read is refused.
    pub fn wait_for_remote(mut self, wait: Duration) -> Self {
        self.first_load_remote_wait = Some(wait);
        self
    }

    /// A subscription's updated rows also carry `column`'s changes as
    /// splices from the value the previous update showed: see
    /// [`crate::Updated::splices`]. One-shot reads ignore it. TS's runtime
    /// has the same option, but internal (not part of its public API).
    pub fn splices<T, V: ColumnValue>(mut self, column: Column<T, V>) -> Self {
        self.splices.push(column.name().to_owned());
        self
    }

    /// Reads through `view`: a head branch, falling back to its live or
    /// frozen base (TS's `branch` and `base`), as [`Jazz::on_branch`]'s
    /// reads do. `ReadOptions::default().branch(Branch::from("draft").over("main"))`.
    pub fn branch(mut self, view: impl Into<BranchView>) -> Self {
        self.branch = Some(view.into());
        self
    }

    /// The core's read options, refusing what can't be answered: a remote
    /// read with nobody to ask, or a remote read told to wait for remote.
    fn into_core<T: Row>(
        self,
        query: &Query<T>,
        serverless: bool,
        default: DefaultRead,
    ) -> Result<(ReadOpts, Option<BranchView>)> {
        if self.tier == Some(ReadTier::Remote) {
            if self.first_load_remote_wait.is_some() {
                return Err(Error::Config(
                    "wait_for_remote is for local-first reads; a Remote read waits for the server anyway".into(),
                ));
            }
            if serverless {
                return Err(Error::Config(
                    "a Remote read needs a server; this app runs without one".into(),
                ));
            }
        }
        let mut opts = match (self.tier, self.first_load_remote_wait) {
            // No tier: the client's default, as TS resolves it. A server
            // runtime reads Remote whatever the wait says.
            (None, None) => default.opts(query),
            (None, Some(_)) if default == DefaultRead::Remote => default.opts(query),
            (tier, _) => read_opts(query, tier.unwrap_or(ReadTier::LocalFirst)),
        };
        if let (Some(wait), false) = (self.first_load_remote_wait, opts.tier == ReadTier::Remote) {
            opts.first_load = FirstLoad::WaitForRemote {
                timeout_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            };
        }
        opts.splices = self.splices;
        Ok((opts, self.branch))
    }
}

impl Jazz {
    /// Every row `query` matches, read as `options` say.
    pub async fn all_with_options<T: Row>(
        &self,
        query: Query<T>,
        options: ReadOptions,
    ) -> Result<Vec<T>> {
        let (opts, branch) =
            options.into_core(&query, self.info.serverless, self.info.default_read)?;
        let query = query.into_untyped()?;
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let client = BranchView::scope(branch.as_ref(), client)?;
            let rows = client.query_with_opts(query, opts).await?;
            rows.into_iter()
                .map(|row| decode_result::<T>(row, &implementations))
                .collect()
        })
        .await
    }

    /// [`Jazz::subscribe`], run as `options` say.
    pub async fn subscribe_with_options<T: Row>(
        &self,
        query: Query<T>,
        options: ReadOptions,
    ) -> Result<Subscription<T>> {
        let (opts, branch) =
            options.into_core(&query, self.info.serverless, self.info.default_read)?;
        let query = query.into_untyped()?;
        if jazz::schema::history::is_history_table(&query.table) {
            return Err(Error::Unsupported(
                "subscribing to history; read it with Jazz::all".into(),
            ));
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let client = BranchView::scope(branch.as_ref(), client)?;
            let stream = client.subscribe_with_opts(query, opts).await?;
            tokio::task::spawn_local(subscription::forward::<T>(stream, sender, implementations));
            Ok(())
        })
        .await?;
        Ok(Subscription::new(receiver, self.clone()))
    }
}
