//! Editing text columns: a mergeable (`"merge": "text"`) column's new value
//! as an edit of the value its writer saw, so concurrent edits merge. On
//! its own ([`Jazz::edit_text`]) or with an update's other columns
//! ([`Jazz::update_edited`], with [`EditedFrom`]).

use jazz::db::LargeValueUpdate;
use jazz::tools::{ObjectId, Value};

use crate::error::Result;
use crate::handle::Jazz;
use crate::query::Column;
use crate::table::Update;
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

/// Which of an update's text columns are edits, and of what: TS's
/// `update(…, { editedFrom })`. Each column named here takes its new value
/// from the update (which must set it to a string), applied as an edit of
/// the value given here, as [`Jazz::edit_text`] does; the update's other
/// columns are set as usual, all in one row update.
///
/// ```ignore
/// jazz.update_edited(
///     id,
///     NoteUpdate { body: Some(next), title: Some(title), ..Default::default() },
///     EditedFrom::new().column(notes::body(), shown),
/// ).await?;
/// ```
pub struct EditedFrom<T> {
    columns: Vec<(&'static str, String)>,
    table: std::marker::PhantomData<fn() -> T>,
}

impl<T> Default for EditedFrom<T> {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            table: std::marker::PhantomData,
        }
    }
}

impl<T> Clone for EditedFrom<T> {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            table: std::marker::PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for EditedFrom<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.columns.iter().map(|(c, v)| (c, v)))
            .finish()
    }
}

impl<T> EditedFrom<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// `column`'s new value is an edit of `edited_from`, the value its
    /// writer saw.
    pub fn column<V: TextColumn>(
        mut self,
        column: Column<T, V>,
        edited_from: impl Into<String>,
    ) -> Self {
        let name = column.name();
        let edited_from = edited_from.into();
        match self.columns.iter_mut().find(|(c, _)| *c == name) {
            Some(entry) => entry.1 = edited_from,
            None => self.columns.push((name, edited_from)),
        }
        self
    }

    fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

type Cells = Vec<(String, Value)>;

/// Splits `cells` into the plain sets and the text edits `edited_from`
/// names, as TS lowers `editedFrom`.
fn lower<T: crate::Table>(
    mut cells: Cells,
    edited_from: EditedFrom<T>,
) -> Result<(Cells, Vec<LargeValueUpdate>)> {
    let mut edits = Vec::with_capacity(edited_from.columns.len());
    for (column, from) in edited_from.columns {
        let at = cells.iter().position(|(name, _)| name == column);
        let text = match at.map(|at| cells.remove(at).1) {
            Some(Value::Text(text)) => text,
            _ => {
                return Err(crate::Error::Config(format!(
                    "{}.{column} is edited from a value: the update must set it to a string",
                    T::NAME
                )));
            }
        };
        edits.push(LargeValueUpdate::TextEdit {
            column: column.to_owned(),
            text,
            edited_from: from,
        });
    }
    Ok((cells, edits))
}

impl Jazz {
    /// Updates the row with this id, as [`Jazz::update`], with the text
    /// columns `edited_from` names applied as edits of the values given
    /// there, as [`Jazz::edit_text`] does: TS's
    /// `update(table, id, data, { editedFrom })`. The sets and the edits
    /// commit as one row update; a failing edit leaves the row unchanged.
    pub async fn update_edited<U: Update>(
        &self,
        id: ObjectId,
        update: U,
        edited_from: EditedFrom<U::Row>,
    ) -> Result<()> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let cells = update.into_cells(&implementations)?;
            let written = if edited_from.is_empty() {
                client.update(<U::Row as crate::Table>::NAME, id, cells)?
            } else {
                let (cells, edits) = lower::<U::Row>(cells, edited_from)?;
                client
                    .update_with_edits(<U::Row as crate::Table>::NAME, id, cells, edits)
                    .map_err(|error| not_found::<U::Row>(error, id))?
            };
            settle.settle(&client, written).await
        })
        .await
    }
}

impl Transaction {
    /// [`Jazz::update_edited`] staged in the transaction.
    pub fn update_edited<U: Update>(
        &self,
        id: ObjectId,
        update: U,
        edited_from: EditedFrom<U::Row>,
    ) -> Result<()> {
        let cells = update.into_cells(self.implementations())?;
        let (cells, edits) = lower::<U::Row>(cells, edited_from)?;
        self.client()
            .update_with_edits(<U::Row as crate::Table>::NAME, id, cells, edits)
            .map_err(|error| not_found::<U::Row>(error, id))?;
        self.wrote();
        Ok(())
    }
}

impl crate::transaction::OpenTransaction {
    /// [`Jazz::update_edited`] staged in the transaction.
    pub async fn update_edited<U: Update>(
        &self,
        id: ObjectId,
        update: U,
        edited_from: EditedFrom<U::Row>,
    ) -> Result<()> {
        self.on(move |tx| async move { tx.update_edited(id, update, edited_from) })
            .await
    }
}
