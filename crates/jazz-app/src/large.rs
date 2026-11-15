//! Large values: `bytes`, `string` and `json` cells edited in place, as
//! TS's `update(…, { applyDiffs })` does, and inserted from a stream, as its
//! `insertStreaming` does. An edit adds to a chunked cell's bounded edit
//! tail instead of rewriting it.

use jazz::db::{JsonSetEdit, LargeValueUpdate};
pub use jazz::db::{LargeValueUpdatePage as Page, LargeValueUpdateSplice as Splice};
use jazz::tools::ObjectId;

use crate::error::Result;
use crate::handle::Jazz;
use crate::query::Column;
use crate::table::{Insert, Table};
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
        let settle = self.settler();
        self.with_client(move |client| async move {
            let written = client.update_with_edits(T::NAME, id, Vec::new(), vec![edit])?;
            settle.settle(&client, written).await
        })
        .await
    }

    /// Inserts `new` with its bytes column `column` read from `reader`, so a
    /// large value never has to be in memory at once. Whatever `new` sets
    /// for `column` is ignored. Not inside transactions.
    pub async fn insert_streaming<N, V, R>(
        &self,
        new: N,
        column: Column<N::Row, V>,
        reader: R,
    ) -> Result<N::Row>
    where
        N: Insert,
        V: LargeColumn,
        R: std::io::Read + Send + 'static,
    {
        let implementations = self.info.implementations.clone();
        let settle = self.settler();
        let name = column.name();
        self.with_client(move |client| async move {
            let mut cells = new.into_cells(&implementations)?;
            cells.remove(name);
            let (id, written) = client
                .insert_streaming_value(<N::Row as Table>::NAME, cells, name, reader)
                .await?;
            settle.settle(&client, Some(written)).await?;
            read_back::<N::Row>(&client, id, &implementations).await
        })
        .await
    }
}
