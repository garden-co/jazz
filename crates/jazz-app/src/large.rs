//! Large values: `bytes`, `string` and `json` cells edited in place, as
//! TS's `update(…, { applyDiffs })` does, and written from an async stream,
//! as its `insertStreaming` does. An edit adds to a chunked cell's bounded
//! edit tail instead of rewriting it.
//!
//! Not supported yet: splices or json edits combined with whole-column
//! replacements in one update (text edits combine with them: see
//! [`Jazz::update_edited`]).

use futures_core::Stream;
use futures_util::StreamExt as _;
use jazz::db::{JsonSetEdit, LargeValueUpdate};
pub use jazz::db::{LargeValueUpdatePage as Page, LargeValueUpdateSplice as Splice};
use jazz::tools::{ObjectId, StreamingUpload, Value};

use crate::error::{Error, Result};
use crate::handle::Jazz;
use crate::query::Column;
use crate::table::{Insert, Table, Update};
use crate::value::{ColumnValue, Json};
use crate::writes::read_back;

/// A column whose cells can be large: bytes and text, nullable or not.
pub trait LargeColumn: ColumnValue {}
impl LargeColumn for Vec<u8> {}
impl LargeColumn for Option<Vec<u8>> {}
impl LargeColumn for String {}
impl LargeColumn for Option<String> {}

/// A json column, nullable or not.
pub trait JsonColumn: ColumnValue {}
impl JsonColumn for Json {}
impl JsonColumn for Option<Json> {}

impl Jazz {
    /// Splices within one page of a bytes or text cell: `splices` apply in
    /// order, each at a page-relative offset, as TS's `applyDiffs` does.
    pub async fn splice<T: Table, V: LargeColumn>(
        &self,
        id: ObjectId,
        column: Column<T, V>,
        within: Page,
        splices: Vec<Splice>,
    ) -> Result<()> {
        let edit = LargeValueUpdate::Splice {
            column: column.name().to_owned(),
            within,
            splices,
        };
        self.apply_edit::<T>(id, edit).await
    }

    /// Replaces members of a json cell by RFC 6901 pointer (`"/a/0"`).
    /// Missing parents are never created.
    pub async fn json_set<T: Table, V: JsonColumn>(
        &self,
        id: ObjectId,
        column: Column<T, V>,
        edits: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Result<()> {
        let edit = LargeValueUpdate::JsonSet {
            column: column.name().to_owned(),
            edits: edits
                .into_iter()
                .map(|(at, value)| JsonSetEdit { at, value })
                .collect(),
        };
        self.apply_edit::<T>(id, edit).await
    }

    async fn apply_edit<T: Table>(&self, id: ObjectId, edit: LargeValueUpdate) -> Result<()> {
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let written = client
                .update_with_edits(T::NAME, id, Vec::new(), vec![edit])
                .map_err(|error| crate::edit::not_found::<T>(error, id))?;
            settle.settle(&client, written).await
        })
        .await
    }

    /// Inserts `new` with its bytes or text column `column` read from
    /// `chunks`, an async stream, as TS's `insertStreaming` does: the value
    /// is never held whole. Each chunk is stored as it arrives, between the
    /// client's other work. `new` must leave `column` unset (`None`, or
    /// empty for a required column). An error from the stream abandons the
    /// upload and fails with [`Error::Upload`]. In a transaction, use
    /// [`crate::Transaction::insert_streaming`] or
    /// [`crate::OpenTransaction::insert_streaming`].
    pub async fn insert_streaming<N, V, S, B, E>(
        &self,
        new: N,
        column: Column<N::Row, V>,
        chunks: S,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let cells = new.into_cells(&self.info.implementations)?;
        self.stream::<N::Row, _, _, _>(
            Kind::Insert,
            None,
            cells.into_iter().collect(),
            column.name(),
            chunks,
        )
        .await
    }

    /// Replaces `column` of the row with this id with `chunks`' bytes, and
    /// `update`'s other columns, in one row update.
    pub async fn update_streaming<U, V, S, B, E>(
        &self,
        id: ObjectId,
        update: U,
        column: Column<U::Row, V>,
        chunks: S,
    ) -> Result<U::Row>
    where
        U: Update,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let cells = update.into_cells(&self.info.implementations)?;
        self.stream::<U::Row, _, _, _>(Kind::Update, Some(id), cells, column.name(), chunks)
            .await
    }

    /// [`Jazz::insert_streaming`] of a row with this id, or
    /// [`Jazz::update_streaming`] of it when it exists.
    pub async fn upsert_streaming<N, V, S, B, E>(
        &self,
        id: ObjectId,
        new: N,
        column: Column<N::Row, V>,
        chunks: S,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let cells = new.into_cells(&self.info.implementations)?;
        self.stream::<N::Row, _, _, _>(
            Kind::Upsert,
            Some(id),
            cells.into_iter().collect(),
            column.name(),
            chunks,
        )
        .await
    }

    async fn stream<T: Table, S, B, E>(
        &self,
        kind: Kind,
        id: Option<ObjectId>,
        mut cells: Vec<(String, Value)>,
        column: &'static str,
        chunks: S,
    ) -> Result<T>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        streamed_column::<T>(&mut cells, column)?;
        let settle = self.settler()?;
        let upload = self
            .with_client(move |client| async move {
                let upload =
                    client.begin_streaming_upload(T::NAME, cells.into_iter().collect(), column)?;
                Ok(park(upload))
            })
            .await?;
        let guard = self.feed(upload, chunks).await?;
        let implementations = self.info.implementations.clone();
        let read = self
            .with_client(move |client| async move {
                let parked = unpark(upload).ok_or(Error::Stopped)?;
                let (id, written) = client
                    .finish_streaming_upload(parked, kind.core(), id)
                    .await?;
                settle.settle(&client, written).await?;
                read_back::<T>(&client, id, &implementations).await
            })
            .await;
        guard.disarm();
        read
    }

    /// Pushes `chunks` into parked upload `upload`. Returns its guard,
    /// still armed: the caller disarms it once the upload is finished (or
    /// unparked). Dropping this future mid-stream (a timeout, a disconnect)
    /// or a failing stream aborts the upload.
    async fn feed<S, B, E>(&self, upload: u64, chunks: S) -> Result<UploadGuard>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let guard = UploadGuard {
            jazz: self.clone(),
            key: upload,
            armed: true,
        };
        let mut chunks = std::pin::pin!(chunks);
        while let Some(chunk) = chunks.next().await {
            // On an error, the guard releases what the upload stored.
            let chunk = chunk.map_err(|error| Error::Upload(error.to_string()))?;
            // One job per piece, and pieces of at most PUSH_BYTES: the
            // client's other calls run in between, however large a chunk.
            let bytes = chunk.as_ref();
            if bytes.len() > PUSH_BYTES {
                let pieces: Vec<Vec<u8>> = bytes.chunks(PUSH_BYTES).map(<[u8]>::to_vec).collect();
                for piece in pieces {
                    self.push(upload, piece).await?;
                }
            } else {
                self.push(upload, chunk).await?;
            }
        }
        Ok(guard)
    }
}

/// The stream carries `column`'s value: the row's cells may only leave it
/// unset. Removes it from `cells`.
fn streamed_column<T: Table>(cells: &mut Vec<(String, Value)>, column: &str) -> Result<()> {
    if let Some(at) = cells.iter().position(|(name, _)| name == column) {
        match &cells[at].1 {
            Value::Null => {}
            Value::Bytea(bytes) if bytes.is_empty() => {}
            Value::Text(text) if text.is_empty() => {}
            _ => {
                return Err(Error::Config(format!(
                    "{}.{column} is streamed: leave it unset in the row and pass its value as the stream",
                    T::NAME
                )));
            }
        }
        cells.remove(at);
    }
    Ok(())
}

impl crate::transaction::Transaction {
    /// [`Jazz::insert_streaming`] staged in the transaction, as TS's
    /// `tx.insertStreaming`: the streamed row commits with the
    /// transaction, rolls back with it, and the transaction sees it once
    /// this returns. A failing stream abandons the upload and fails with
    /// [`Error::Upload`], staging nothing.
    pub async fn insert_streaming<N, V, S, B, E>(
        &self,
        new: N,
        column: Column<N::Row, V>,
        chunks: S,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        let cells = new.into_cells(self.implementations())?;
        self.stream::<N::Row, _, _, _>(
            Kind::Insert,
            None,
            cells.into_iter().collect(),
            column.name(),
            chunks,
        )
        .await
    }

    /// [`Jazz::update_streaming`] staged in the transaction.
    pub async fn update_streaming<U, V, S, B, E>(
        &self,
        id: ObjectId,
        update: U,
        column: Column<U::Row, V>,
        chunks: S,
    ) -> Result<U::Row>
    where
        U: Update,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        let cells = update.into_cells(self.implementations())?;
        self.stream::<U::Row, _, _, _>(Kind::Update, Some(id), cells, column.name(), chunks)
            .await
    }

    /// [`Jazz::upsert_streaming`] staged in the transaction.
    pub async fn upsert_streaming<N, V, S, B, E>(
        &self,
        id: ObjectId,
        new: N,
        column: Column<N::Row, V>,
        chunks: S,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        let cells = new.into_cells(self.implementations())?;
        self.stream::<N::Row, _, _, _>(
            Kind::Upsert,
            Some(id),
            cells.into_iter().collect(),
            column.name(),
            chunks,
        )
        .await
    }

    /// The closure form runs on the client's thread: it feeds the upload
    /// directly, each push awaiting its turn as any local task does. If
    /// the body is dropped mid-stream (it panicked), dropping the upload
    /// releases what it stored.
    async fn stream<T: Table, S, B, E>(
        &self,
        kind: Kind,
        id: Option<ObjectId>,
        mut cells: Vec<(String, Value)>,
        column: &'static str,
        chunks: S,
    ) -> Result<T>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        streamed_column::<T>(&mut cells, column)?;
        let client = self.client();
        let mut upload =
            client.begin_streaming_upload(T::NAME, cells.into_iter().collect(), column)?;
        let mut chunks = std::pin::pin!(chunks);
        while let Some(chunk) = chunks.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = client.abort_streaming_upload(upload).await;
                    return Err(Error::Upload(error.to_string()));
                }
            };
            for piece in chunk.as_ref().chunks(PUSH_BYTES) {
                // A failed push closed the upload; it is dropped here.
                client.push_streaming_upload(&mut upload, piece).await?;
            }
        }
        let (id, _) = client
            .finish_streaming_upload(upload, kind.core(), id)
            .await?;
        self.wrote();
        self.read_back(id).await
    }

    /// [`Jazz::splice`] staged in the transaction.
    pub fn splice<T: Table, V: LargeColumn>(
        &self,
        id: ObjectId,
        column: Column<T, V>,
        within: Page,
        splices: Vec<Splice>,
    ) -> Result<()> {
        let edit = LargeValueUpdate::Splice {
            column: column.name().to_owned(),
            within,
            splices,
        };
        self.client()
            .update_with_edits(T::NAME, id, Vec::new(), vec![edit])
            .map_err(|error| crate::edit::not_found::<T>(error, id))?;
        self.wrote();
        Ok(())
    }

    /// [`Jazz::json_set`] staged in the transaction.
    pub fn json_set<T: Table, V: JsonColumn>(
        &self,
        id: ObjectId,
        column: Column<T, V>,
        edits: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Result<()> {
        let edit = LargeValueUpdate::JsonSet {
            column: column.name().to_owned(),
            edits: edits
                .into_iter()
                .map(|(at, value)| JsonSetEdit { at, value })
                .collect(),
        };
        self.client()
            .update_with_edits(T::NAME, id, Vec::new(), vec![edit])
            .map_err(|error| crate::edit::not_found::<T>(error, id))?;
        self.wrote();
        Ok(())
    }
}

impl crate::transaction::OpenTransaction {
    /// [`Jazz::insert_streaming`] staged in the transaction: it commits
    /// with [`crate::OpenTransaction::commit`] and rolls back with it. Each
    /// chunk is stored as it arrives, between the client's other work.
    /// Dropping this call mid-stream aborts the upload and leaves the
    /// transaction as it was; a failing stream does too, failing with
    /// [`Error::Upload`].
    pub async fn insert_streaming<N, V, S, B, E>(
        &self,
        new: N,
        column: Column<N::Row, V>,
        chunks: S,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let cells = new.into_cells(&self.jazz.info.implementations)?;
        self.stream::<N::Row, _, _, _>(
            Kind::Insert,
            None,
            cells.into_iter().collect(),
            column.name(),
            chunks,
        )
        .await
    }

    /// [`Jazz::update_streaming`] staged in the transaction.
    pub async fn update_streaming<U, V, S, B, E>(
        &self,
        id: ObjectId,
        update: U,
        column: Column<U::Row, V>,
        chunks: S,
    ) -> Result<U::Row>
    where
        U: Update,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let cells = update.into_cells(&self.jazz.info.implementations)?;
        self.stream::<U::Row, _, _, _>(Kind::Update, Some(id), cells, column.name(), chunks)
            .await
    }

    /// [`Jazz::upsert_streaming`] staged in the transaction.
    pub async fn upsert_streaming<N, V, S, B, E>(
        &self,
        id: ObjectId,
        new: N,
        column: Column<N::Row, V>,
        chunks: S,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        let cells = new.into_cells(&self.jazz.info.implementations)?;
        self.stream::<N::Row, _, _, _>(
            Kind::Upsert,
            Some(id),
            cells.into_iter().collect(),
            column.name(),
            chunks,
        )
        .await
    }

    async fn stream<T: Table, S, B, E>(
        &self,
        kind: Kind,
        id: Option<ObjectId>,
        mut cells: Vec<(String, Value)>,
        column: &'static str,
        chunks: S,
    ) -> Result<T>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]> + Send + 'static,
        E: std::fmt::Display,
    {
        streamed_column::<T>(&mut cells, column)?;
        let upload = self
            .on(move |tx| async move {
                let upload = tx.client().begin_streaming_upload(
                    T::NAME,
                    cells.into_iter().collect(),
                    column,
                )?;
                Ok(park(upload))
            })
            .await?;
        let guard = self.jazz.feed(upload, chunks).await?;
        // If the transaction ended meanwhile, the upload stays parked and
        // the guard aborts it.
        let read = self
            .on(move |tx| async move {
                let parked = unpark(upload).ok_or(Error::Stopped)?;
                let (id, _) = tx
                    .client()
                    .finish_streaming_upload(parked, kind.core(), id)
                    .await?;
                tx.wrote();
                tx.read_back(id).await
            })
            .await;
        guard.disarm();
        read
    }
}

/// The most bytes one job pushes, so a large chunk doesn't hold the client
/// for long.
const PUSH_BYTES: usize = 64 * 1024;

impl Jazz {
    async fn push<B: AsRef<[u8]> + Send + 'static>(&self, upload: u64, bytes: B) -> Result<()> {
        self.with_client(move |client| async move {
            let mut parked = unpark(upload).ok_or(Error::Stopped)?;
            let pushed = client
                .push_streaming_upload(&mut parked, bytes.as_ref())
                .await;
            // A failed push closed the upload; it is dropped here.
            pushed?;
            repark(upload, parked);
            Ok(())
        })
        .await
    }

    /// Uploads in progress on the client's thread (for tests).
    #[doc(hidden)]
    pub async fn uploads_in_progress(&self) -> Result<usize> {
        self.with_client(|_| async { Ok(UPLOADS.with(|uploads| uploads.borrow().1.len())) })
            .await
    }
}

/// Aborts a parked upload when the call streaming it ends early.
pub(crate) struct UploadGuard {
    jazz: Jazz,
    key: u64,
    armed: bool,
}

impl UploadGuard {
    /// The upload is finished or handed on: nothing to abort.
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for UploadGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let key = self.key;
        // Fire and forget: the abort runs on the client's thread.
        self.jazz.fire(move |client| async move {
            if let Some(upload) = unpark(key) {
                let _ = client.abort_streaming_upload(upload).await;
            }
        });
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Insert,
    Update,
    Upsert,
}

impl Kind {
    pub(crate) fn core(self) -> jazz::db::StreamingMutationKind {
        match self {
            Kind::Insert => jazz::db::StreamingMutationKind::Insert,
            Kind::Update => jazz::db::StreamingMutationKind::Update,
            Kind::Upsert => jazz::db::StreamingMutationKind::Upsert,
        }
    }
}

// Uploads in progress live on the client's thread, between the jobs that
// feed them. Unlike parked transactions, one still here when the thread
// exits (shutdown mid-upload) is safe to drop with it: an upload holds no
// client, only weak references to the store, and its staged chunks are
// reclaimed when its lease expires (`shutdown_mid_upload` covers it).
thread_local! {
    static UPLOADS: std::cell::RefCell<(u64, std::collections::HashMap<u64, StreamingUpload>)> =
        Default::default();
}

pub(crate) fn park(upload: StreamingUpload) -> u64 {
    UPLOADS.with(|uploads| {
        let mut uploads = uploads.borrow_mut();
        uploads.0 += 1;
        let key = uploads.0;
        uploads.1.insert(key, upload);
        key
    })
}

pub(crate) fn unpark(key: u64) -> Option<StreamingUpload> {
    UPLOADS.with(|uploads| uploads.borrow_mut().1.remove(&key))
}

fn repark(key: u64, upload: StreamingUpload) {
    UPLOADS.with(|uploads| {
        uploads.borrow_mut().1.insert(key, upload);
    });
}
