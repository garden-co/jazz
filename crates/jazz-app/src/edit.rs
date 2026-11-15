//! Editing text columns: a mergeable (`"merge": "text"`) column's new value
//! as an edit of the value its writer saw, so concurrent edits merge.

use jazz::db::LargeValueUpdate;
use jazz::tools::ObjectId;

use crate::error::Result;
use crate::handle::Jazz;
use crate::query::Column;
use crate::transaction::Transaction;

/// A text column's type: `String`, or `Option<String>` for a nullable one.
pub trait TextColumn: crate::value::ColumnValue {}
impl TextColumn for String {}
impl TextColumn for Option<String> {}

fn text_edit<T, V: TextColumn>(
    column: Column<T, V>,
    edited_from: String,
    text: String,
) -> LargeValueUpdate {
    LargeValueUpdate::TextEdit {
        column: column.name().to_owned(),
        text,
        edited_from,
    }
}

impl Jazz {
    /// Sets text column `column` of the row with this id to `text`, as an
    /// edit of `edited_from`: the value the writer started from (what an
    /// editor showed when typing began). If the column has changed since,
    /// on a column with `"merge": "text"`, the edit is merged over it, as
    /// the server merges, so it keeps what the writer didn't see.
    pub async fn edit_text<T: crate::Table, V: TextColumn>(
        &self,
        id: ObjectId,
        column: Column<T, V>,
        edited_from: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<()> {
        let edit = text_edit(column, edited_from.into(), text.into());
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let written = client.update_with_edits(T::NAME, id, Vec::new(), vec![edit])?;
            settle.settle(&client, written).await
        })
        .await
    }
}

impl Transaction {
    /// [`Jazz::edit_text`] staged in the transaction.
    pub fn edit_text<T: crate::Table, V: TextColumn>(
        &self,
        id: ObjectId,
        column: Column<T, V>,
        edited_from: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<()> {
        let edit = text_edit(column, edited_from.into(), text.into());
        self.client()
            .update_with_edits(T::NAME, id, Vec::new(), vec![edit])?;
        self.wrote();
        Ok(())
    }
}
