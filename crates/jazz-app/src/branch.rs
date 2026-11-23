//! Branches and drafts, as TS's `branch` and `base` options and
//! `captureSnapshot`: reading through a branch view, writing into a branch,
//! and freezing a base at a snapshot.
//!
//! A branch is a coordinate: values of the schema's `branchBy` columns,
//! which are ordinary columns. [`Jazz::on_branch`] gives a handle whose
//! reads see the head branch's rows, falling back to its base's, and whose
//! writes land in the head:
//!
//! ```ignore
//! let draft = jazz.on_branch(Branch::from("draft").over("main"));
//! let docs = draft.all(documents::query()).await?;    // draft, else main
//! draft.update(doc.id, DocumentUpdate { title: Some("Reworked".into()), ..Default::default() }).await?;
//!
//! // A frozen base: main as it was when the draft was made.
//! let at = jazz.capture_snapshot().await?;
//! let draft = jazz.on_branch(Branch::from("draft").over_snapshot("main", at));
//! ```

use std::fmt;
use std::str::FromStr;

use jazz::protocol::{BranchSelector, BranchViewBase, SnapshotRef};
use jazz::tools::{DurabilityTier, JazzClient, JazzError, ObjectId, ReadTier, Value};
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::handle::{Jazz, decode_result, insert, read_opts};
use crate::include::Row;
use crate::query::{Column, Query};
use crate::read::ReadOptions;
use crate::subscription::{self, Subscription};
use crate::table::{Insert, Table, Updatable, Update};
use crate::transaction::Transaction;
use crate::value::ColumnValue;
use crate::writes::read_back;

/// A branch coordinate: a value for each of the schema's branch columns,
/// as TS's `Branch`.
///
/// When the schema has one branch column, its value alone names the branch
/// (`Branch::from("draft")`, `Branch::new(branch_id)`). Otherwise name each
/// column: `Branch::by(documents::workspace_id(), ws).and(documents::branch_id(), id)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    /// `None`: the schema's one branch column.
    values: Vec<(Option<String>, Value)>,
}

impl Branch {
    /// The branch whose value of the schema's one branch column is `value`
    /// (TS's scalar `branch`). Using it fails when the schema has more or
    /// fewer than one branch column.
    pub fn new(value: impl ColumnValue) -> Self {
        Self {
            values: vec![(None, value.into_value())],
        }
    }

    /// The branch where `column` is `value`; add the schema's other branch
    /// columns with [`Branch::and`] (TS's qualified `branch`).
    pub fn by<T, V: ColumnValue>(column: Column<T, V>, value: impl Into<V>) -> Self {
        Self { values: Vec::new() }.and(column, value)
    }

    /// [`Branch::by`] for a column named at run time.
    pub fn named(column: impl Into<String>, value: impl ColumnValue) -> Self {
        Self { values: Vec::new() }.and_named(column, value)
    }

    /// This branch, also with `column` at `value`.
    pub fn and<T, V: ColumnValue>(self, column: Column<T, V>, value: impl Into<V>) -> Self {
        self.and_named(column.name(), value.into())
    }

    /// [`Branch::and`] for a column named at run time.
    pub fn and_named(mut self, column: impl Into<String>, value: impl ColumnValue) -> Self {
        self.values.push((Some(column.into()), value.into_value()));
        self
    }

    /// The view of this branch over `base`'s current rows: TS's
    /// `{ branch, base }`.
    pub fn over(self, base: impl Into<Branch>) -> BranchView {
        BranchView {
            head: self,
            base: Some((base.into(), None)),
        }
    }

    /// The view of this branch over `base` as it was at `snapshot`
    /// ([`Jazz::capture_snapshot`]): TS's `{ branch, base: [base, snapshot] }`.
    pub fn over_snapshot(self, base: impl Into<Branch>, snapshot: Snapshot) -> BranchView {
        BranchView {
            head: self,
            base: Some((base.into(), Some(snapshot))),
        }
    }

    fn selector(&self, client: &JazzClient) -> Result<BranchSelector> {
        let values = match self.values.as_slice() {
            [(None, value)] => match client.branch_columns().as_slice() {
                [column] => vec![(column.clone(), value.clone())],
                columns => {
                    return Err(JazzError::Schema(format!(
                        "a branch given by its value alone needs exactly one branch column \
                         in the schema, which has {}; name them with Branch::by",
                        columns.len()
                    ))
                    .into());
                }
            },
            values => values
                .iter()
                .map(|(name, value)| {
                    name.clone()
                        .map(|name| (name, value.clone()))
                        .ok_or_else(|| {
                            Error::from(JazzError::Schema(
                                "a branch is given by its value alone or by its columns, not both"
                                    .into(),
                            ))
                        })
                })
                .collect::<Result<_>>()?,
        };
        Ok(client.branch_selector(values)?)
    }
}

impl From<&str> for Branch {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

impl From<String> for Branch {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<ObjectId> for Branch {
    fn from(value: ObjectId) -> Self {
        Self::new(value)
    }
}

impl From<i32> for Branch {
    fn from(value: i32) -> Self {
        Self::new(value)
    }
}

impl From<i64> for Branch {
    fn from(value: i64) -> Self {
        Self::new(value)
    }
}

/// A head branch, optionally over a base it falls back to: what
/// [`Jazz::on_branch`] reads and writes through.
#[derive(Debug, Clone, PartialEq)]
pub struct BranchView {
    head: Branch,
    /// The base, frozen at a snapshot or (`None`) live.
    base: Option<(Branch, Option<Snapshot>)>,
}

impl BranchView {
    /// `client`, through `view` when there is one.
    pub(crate) fn scope(view: Option<&BranchView>, client: JazzClient) -> Result<JazzClient> {
        match view {
            Some(view) => view.resolve(&client),
            None => Ok(client),
        }
    }

    fn resolve(&self, client: &JazzClient) -> Result<JazzClient> {
        let head = self.head.selector(client)?;
        let base = match &self.base {
            None => None,
            Some((branch, None)) => Some(BranchViewBase::current(branch.selector(client)?)),
            Some((branch, Some(snapshot))) => Some(BranchViewBase::snapshot(
                branch.selector(client)?,
                snapshot.0.clone(),
            )),
        };
        Ok(client.on_branch(head, base))
    }
}

impl<B: Into<Branch>> From<B> for BranchView {
    fn from(head: B) -> Self {
        Self {
            head: head.into(),
            base: None,
        }
    }
}

/// A frozen read frontier of this store ([`Jazz::capture_snapshot`]), for
/// a base that keeps showing the data as it was.
///
/// Store it as a string ([`Snapshot::to_persisted`], or `to_string`) in a
/// column of the branch's row, and parse it back with
/// [`Snapshot::from_persisted`] (or `parse`): the format TS's
/// `serializeSnapshotRef` writes and `parseSnapshotRef` reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Snapshot(SnapshotRef);

impl Snapshot {
    pub fn to_persisted(&self) -> String {
        self.0.to_persisted()
    }

    pub fn from_persisted(persisted: &str) -> Result<Self> {
        SnapshotRef::from_persisted(persisted)
            .map(Self)
            .map_err(|error| Error::App(format!("not a snapshot reference: {error}")))
    }
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_persisted())
    }
}

impl FromStr for Snapshot {
    type Err = Error;
    fn from_str(persisted: &str) -> Result<Self> {
        Self::from_persisted(persisted)
    }
}

impl Jazz {
    /// This app, read and written through `view`, as TS's `branch` and
    /// `base` options: `jazz.on_branch(Branch::from("draft").over("main"))`.
    ///
    /// Its reads and subscriptions see the head branch's rows, falling back
    /// to the base's; the head's deletions hide inherited rows. Its
    /// updates, upserts and deletes land in the head, copying an inherited
    /// row there first, and leave the base as it is. Its inserts and
    /// restores land in the head too. Tables without `branchBy` are shared
    /// by every branch: their rows are read and written as they are.
    pub fn on_branch(&self, view: impl Into<BranchView>) -> OnBranch {
        OnBranch {
            jazz: self.clone(),
            view: view.into(),
        }
    }

    /// Captures this store's local read frontier, for a frozen base
    /// ([`Branch::over_snapshot`]), as TS's `captureSnapshot`.
    ///
    /// It includes the writes committed locally so far, whether the server
    /// accepted them yet or not, and none made later. It retains no history
    /// and grants no permission: reading at it later needs the same dataset,
    /// with enough history kept.
    pub async fn capture_snapshot(&self) -> Result<Snapshot> {
        self.with_client(|client| async move { Ok(Snapshot(client.capture_snapshot().await?)) })
            .await
    }
}

/// An app handle that reads and writes through a branch view
/// ([`Jazz::on_branch`]). `Send + Clone`, like [`Jazz`].
///
/// Not supported yet through a branch: streaming inserts and updates, and
/// byte-range reads; they fail. It has no `transaction` of its own: use
/// [`Jazz::transaction`] and [`Transaction::on_branch`]. [`Jazz::wait_for_sync`]
/// covers its writes.
///
/// Where it differs from TS: a selector names every branch column of the
/// schema (or is the value of its only one), for reads and writes alike;
/// and a write to a table without `branchBy` goes to its shared rows, where
/// TS refuses a `branch` option on such a table.
#[derive(Debug, Clone)]
pub struct OnBranch {
    jazz: Jazz,
    view: BranchView,
}

impl OnBranch {
    /// The view this handle reads and writes through.
    pub fn view(&self) -> &BranchView {
        &self.view
    }

    /// This handle, with writes that return once they reached `tier`, as
    /// [`Jazz::durable`].
    pub fn durable(&self, tier: DurabilityTier) -> OnBranch {
        OnBranch {
            jazz: self.jazz.durable(tier),
            view: self.view.clone(),
        }
    }

    /// Runs `job` with the untyped client, scoped to this branch view.
    async fn with_branch<F, Fut, R>(&self, job: F) -> Result<R>
    where
        F: FnOnce(JazzClient) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<R>> + 'static,
        R: Send + 'static,
    {
        let view = self.view.clone();
        self.jazz
            .with_client(move |client| async move { job(view.resolve(&client)?).await })
            .await
    }

    /// Every row `query` matches in this view, read as [`Jazz::all`] reads
    /// (at the client's default tier).
    pub async fn all<T: Row>(&self, query: Query<T>) -> Result<Vec<T>> {
        self.jazz
            .all_with_options(query, ReadOptions::default().branch(self.view.clone()))
            .await
    }

    /// Every row `query` matches in this view, read at `tier`.
    pub async fn all_with<T: Row>(&self, query: Query<T>, tier: ReadTier) -> Result<Vec<T>> {
        let opts = read_opts(&query, tier);
        let query = query.into_untyped()?;
        let implementations = self.jazz.info.implementations.clone();
        self.with_branch(move |client| async move {
            let rows = client.query_with_opts(query, opts).await?;
            rows.into_iter()
                .map(|row| decode_result::<T>(row, &implementations))
                .collect()
        })
        .await
    }

    /// The first row `query` matches in this view.
    pub async fn one<T: Row>(&self, query: Query<T>) -> Result<Option<T>> {
        Ok(self.all(query.limit(1)).await?.into_iter().next())
    }

    /// The row of `T` with this id, as this view sees it.
    pub async fn get<T: Table>(&self, id: ObjectId) -> Result<Option<T>> {
        self.one(Query::<T>::all().id(id)).await
    }

    /// Subscribes to `query` in this view, as [`Jazz::subscribe`].
    pub async fn subscribe<T: Row>(&self, query: Query<T>) -> Result<Subscription<T>> {
        self.jazz
            .subscribe_with_options(query, ReadOptions::default().branch(self.view.clone()))
            .await
    }

    /// [`OnBranch::subscribe`] at `tier`.
    pub async fn subscribe_with<T: Row>(
        &self,
        query: Query<T>,
        tier: ReadTier,
    ) -> Result<Subscription<T>> {
        let opts = read_opts(&query, tier);
        let query = query.into_untyped()?;
        if jazz::schema::history::is_history_table(&query.table) {
            return Err(Error::Unsupported(
                "subscribing to history; read it with OnBranch::all".into(),
            ));
        }
        let (sender, receiver) = mpsc::channel(subscription::SUBSCRIPTION_ITEMS);
        let implementations = self.jazz.info.implementations.clone();
        self.with_branch(move |client| async move {
            let stream = client.subscribe_with_opts(query, opts).await?;
            tokio::task::spawn_local(subscription::forward::<T>(stream, sender, implementations));
            Ok(())
        })
        .await?;
        Ok(Subscription::new(receiver, self.jazz.clone()))
    }

    /// Inserts a row into the head branch. Its branch columns must hold
    /// the head's values. Returns it as stored.
    pub async fn insert<N: Insert>(&self, new: N) -> Result<N::Row> {
        let implementations = self.jazz.info.implementations.clone();
        let settle = self.jazz.settler()?;
        self.with_branch(move |client| async move {
            let (row, written) = insert(&client, new, &implementations)?;
            settle.settle(&client, written).await?;
            Ok(row)
        })
        .await
    }

    /// Updates the row with this id in the head branch, copying it there
    /// first when the view inherits it from the base.
    pub async fn update<U: Update>(&self, id: ObjectId, update: U) -> Result<()> {
        let implementations = self.jazz.info.implementations.clone();
        let settle = self.jazz.settler()?;
        self.with_branch(move |client| async move {
            let cells = update.into_cells(&implementations)?;
            let written = client.update(<U::Row as Table>::NAME, id, cells)?;
            settle.settle(&client, written).await
        })
        .await
    }

    /// Writes the row with this id in the head branch, as [`Jazz::upsert`].
    /// Returns it as this view now sees it.
    pub async fn upsert<N: Insert>(&self, id: ObjectId, new: N) -> Result<N::Row>
    where
        N::Row: Updatable,
    {
        let implementations = self.jazz.info.implementations.clone();
        let settle = self.jazz.settler()?;
        self.with_branch(move |client| async move {
            let cells = new.into_cells(&implementations)?;
            let written = client.upsert(<N::Row as Table>::NAME, *id.uuid(), cells)?;
            settle.settle(&client, written).await?;
            read_back::<N::Row>(&client, id, &implementations).await
        })
        .await
    }

    /// Deletes the row of `T` with this id from the head branch, hiding an
    /// inherited row there. The base keeps it.
    pub async fn delete<T: Table>(&self, id: ObjectId) -> Result<()> {
        let settle = self.jazz.settler()?;
        self.with_branch(move |client| async move {
            let written = client.delete(T::NAME, id)?;
            settle.settle(&client, written).await
        })
        .await
    }

    /// Brings back the row of `T` the head branch deleted, as it was.
    pub async fn restore<T: Updatable>(&self, id: ObjectId) -> Result<T> {
        self.restore_cells::<T>(id, None).await
    }

    /// Brings back the row the head branch deleted, with `new`'s values, as
    /// [`Jazz::restore_with`].
    pub async fn restore_with<N: Insert>(&self, id: ObjectId, new: N) -> Result<N::Row>
    where
        N::Row: Updatable,
    {
        let cells = new.into_cells(&self.jazz.info.implementations)?;
        self.restore_cells::<N::Row>(id, Some(cells.into_iter().collect()))
            .await
    }

    async fn restore_cells<T: Updatable>(
        &self,
        id: ObjectId,
        cells: Option<Vec<(String, Value)>>,
    ) -> Result<T> {
        let implementations = self.jazz.info.implementations.clone();
        let settle = self.jazz.settler()?;
        self.with_branch(move |client| async move {
            let written = client.restore(T::NAME, id, cells)?;
            settle.settle(&client, written).await?;
            read_back::<T>(&client, id, &implementations).await
        })
        .await
    }
}

impl Transaction {
    /// This transaction, writing through `view`: its inserts, updates and
    /// deletes land in the head branch, as [`OnBranch`]'s do, and commit
    /// with the rest of the transaction. Writes to several branches (and
    /// to shared tables) in one transaction keep one fate.
    ///
    /// Its reads go through the view too, staged writes included, in a
    /// mergeable transaction ([`Jazz::transaction`]) and a serializable one
    /// ([`Jazz::serializable_transaction`]) alike; a serializable one checks
    /// at commit that the rows it read through the view did not change (on
    /// a server, one that negotiated it: jazz#5806). Its `upsert`,
    /// `restore` and `restore_with` return the row as it now reads through
    /// the view.
    pub fn on_branch(&self, view: impl Into<BranchView>) -> Result<Transaction> {
        let client = view.into().resolve(self.client())?;
        Ok(self.with_client(client))
    }
}
