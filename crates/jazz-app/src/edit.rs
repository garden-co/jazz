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
    ///
    /// `edited_from` is trusted as the base: a wrong one is not an error,
    /// it just makes the merge treat the difference as the writer's edit.
    /// A column without `"merge": "text"` is refused; a missing row fails
    /// with [`crate::Error::NotFound`].
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
            let written = client
                .update_with_edits(T::NAME, id, Vec::new(), vec![edit])
                .map_err(|error| not_found::<T>(error, id))?;
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
            .update_with_edits(T::NAME, id, Vec::new(), vec![edit])
            .map_err(|error| not_found::<T>(error, id))?;
        self.wrote();
        Ok(())
    }
}

/// The core reports an edit of a row it can't see as "not observed": to
/// the caller, there is no such row.
pub(crate) fn not_found<T: crate::Table>(
    error: jazz::tools::JazzError,
    id: ObjectId,
) -> crate::Error {
    if error.to_string().contains("NotObserved") {
        crate::Error::NotFound {
            table: T::NAME,
            id: id.uuid().to_string(),
        }
    } else {
        error.into()
    }
}
