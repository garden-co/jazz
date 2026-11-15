//! Read options, as TS's query options: the tier a read waits for, how long
//! a local-first read waits for the server's first answer, and which large
//! columns a subscription's updates carry as splices.

use std::time::Duration;

use jazz::db::{FirstLoad, ReadOpts};
use jazz::tools::ReadTier;

use crate::error::Result;
use crate::handle::{Jazz, decode_result};
use crate::include::Row;
use crate::query::{Column, Query};
use crate::subscription::{self, Subscription};
use crate::value::ColumnValue;

/// How a read or subscription runs. `ReadOptions::default()` reads local
/// data first and delivers at once.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    tier: Option<ReadTier>,
    first_load_remote_wait: Option<Duration>,
    splices: Vec<String>,
}

impl ReadOptions {
    /// Reads at `tier` (TS's `tier`).
    pub fn tier(mut self, tier: ReadTier) -> Self {
        self.tier = Some(tier);
        self
    }

    /// Reads local data first, but waits up to `wait` for the server's
    /// answer before the first result (TS's `firstLoadRemoteWaitMs`).
    /// Offline, or once `wait` passed, it answers locally.
    pub fn wait_for_remote(mut self, wait: Duration) -> Self {
        self.first_load_remote_wait = Some(wait);
        self
    }

    /// A subscription's updated rows also carry `column`'s changes as
    /// splices from the value the previous update showed (TS's `splices`):
    /// see [`crate::Updated::splices`]. One-shot reads ignore it.
    pub fn splices<T, V: ColumnValue>(mut self, column: Column<T, V>) -> Self {
        self.splices.push(column.name().to_owned());
        self
    }

    fn into_core(self) -> ReadOpts {
        let mut opts = ReadOpts::for_read_tier(self.tier.unwrap_or(ReadTier::LocalFirst));
        if let Some(wait) = self.first_load_remote_wait {
            opts.first_load = FirstLoad::WaitForRemote {
                timeout_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            };
        }
        opts.splices = self.splices;
        opts
    }
}

impl Jazz {
    /// Every row `query` matches, read as `options` say.
    pub async fn all_with_options<T: Row>(
        &self,
        query: Query<T>,
        options: ReadOptions,
    ) -> Result<Vec<T>> {
        let query = query.into_untyped();
        let opts = options.into_core();
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
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
        let query = query.into_untyped();
        let opts = options.into_core();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let implementations = self.info.implementations.clone();
        self.with_client(move |client| async move {
            let stream = client.subscribe_with_opts(query, opts).await?;
            tokio::task::spawn_local(subscription::forward::<T>(stream, sender, implementations));
            Ok(())
        })
        .await?;
        Ok(Subscription::new(receiver, self.clone()))
    }
}
