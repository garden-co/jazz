//! Editing text columns: a mergeable (`"merge": "text"`) column's new value
//! as an edit of the value its writer saw, so concurrent edits merge. On
//! its own ([`Jazz::edit_text`]) or with an update's other columns
//! ([`Jazz::update_edited`], with [`Edits`]).

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
    let message = error.to_string();
    if message.contains("NotObserved") {
        crate::Error::NotFound {
            table: T::NAME,
            id: id.uuid().to_string(),
        }
    } else if let Some(at) = message.find(jazz::db::NOT_CHAR_BOUNDARY) {
        crate::Error::TextOffset(message[at..].to_owned())
    } else {
        error.into()
    }
}

/// How an update edits columns in place, as TS's `update` options do:
/// text columns written as edits of the value their writer saw
/// ([`Edits::edited_from`], TS's `editedFrom`), and splices and json sets
/// ([`Edits::splice`], [`Edits::json_set`], TS's `applyDiffs`). The
/// update's other columns are set as usual, all in one row update.
///
/// ```ignore
/// jazz.update_edited(
///     id,
///     NoteUpdate { body: Some(next), title: Some(title), ..Default::default() },
///     Edits::new()
///         .edited_from(notes::body(), shown)
///         .json_set(notes::meta(), [("/seen".to_owned(), json!(true))]),
/// ).await?;
/// ```
///
/// A later entry for a column replaces an earlier one.
pub struct Edits<T> {
    edits: Vec<Edit>,
    table: std::marker::PhantomData<fn() -> T>,
}

#[derive(Clone, Debug)]
enum Edit {
    /// The update's value of `column`, as an edit of this one.
    Text {
        column: &'static str,
        edited_from: String,
    },
    /// A splice or json set, which the update must leave `column` unset for.
    Diff {
        column: &'static str,
        diff: LargeValueUpdate,
    },
}

impl Edit {
    fn column(&self) -> &'static str {
        match self {
            Edit::Text { column, .. } | Edit::Diff { column, .. } => column,
        }
    }
}

impl<T> Default for Edits<T> {
    fn default() -> Self {
        Self {
            edits: Vec::new(),
            table: std::marker::PhantomData,
        }
    }
}

impl<T> Clone for Edits<T> {
    fn clone(&self) -> Self {
        Self {
            edits: self.edits.clone(),
            table: std::marker::PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for Edits<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(&self.edits).finish()
    }
}

impl<T> Edits<T> {
    pub fn new() -> Self {
        Self::default()
    }

    fn with(mut self, edit: Edit) -> Self {
        let column = edit.column();
        match self.edits.iter_mut().find(|e| e.column() == column) {
            Some(entry) => *entry = edit,
            None => self.edits.push(edit),
        }
        self
    }

    /// `column`'s new value, which the update sets, is an edit of
    /// `edited_from`, the value its writer saw (TS's `editedFrom`).
    pub fn edited_from<V: TextColumn>(
        self,
        column: Column<T, V>,
        edited_from: impl Into<String>,
    ) -> Self {
        self.with(Edit::Text {
            column: column.name(),
            edited_from: edited_from.into(),
        })
    }

    /// Splices within one page of a bytes or text cell, as
    /// [`Jazz::splice`] (TS's `applyDiffs`), in the same units: UTF-8 bytes
    /// on a text column ([`Page::text`](crate::Page::text)). The update
    /// must leave `column` unset.
    pub fn splice<V: crate::LargeColumn>(
        self,
        column: Column<T, V>,
        within: crate::Page,
        splices: Vec<crate::Splice>,
    ) -> Self {
        self.with(Edit::Diff {
            column: column.name(),
            diff: LargeValueUpdate::Splice {
                column: column.name().to_owned(),
                within,
                splices,
            },
        })
    }

    /// Replaces members of a json cell by pointer, as [`Jazz::json_set`]
    /// (TS's `applyDiffs`). The update must leave `column` unset.
    pub fn json_set<V: crate::JsonColumn>(
        self,
        column: Column<T, V>,
        edits: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.with(Edit::Diff {
            column: column.name(),
            diff: LargeValueUpdate::JsonSet {
                column: column.name().to_owned(),
                edits: edits
                    .into_iter()
                    .map(|(at, value)| jazz::db::JsonSetEdit { at, value })
                    .collect(),
            },
        })
    }

    fn is_empty(&self) -> bool {
        self.edits.is_empty()
    }
}

type Cells = Vec<(String, Value)>;

/// Splits `cells` into the plain sets and the edits, as TS lowers
/// `editedFrom` and `applyDiffs`.
fn lower<T: crate::Table>(
    mut cells: Cells,
    edits: Edits<T>,
) -> Result<(Cells, Vec<LargeValueUpdate>)> {
    let mut lowered = Vec::with_capacity(edits.edits.len());
    for edit in edits.edits {
        let at = cells.iter().position(|(name, _)| name == edit.column());
        match edit {
            Edit::Text {
                column,
                edited_from,
            } => {
                let text = match at.map(|at| cells.remove(at).1) {
                    Some(Value::Text(text)) => text,
                    _ => {
                        return Err(crate::Error::Config(format!(
                            "{}.{column} is edited from a value: the update must set it to a string",
                            T::NAME
                        )));
                    }
                };
                lowered.push(LargeValueUpdate::TextEdit {
                    column: column.to_owned(),
                    text,
                    edited_from,
                });
            }
            Edit::Diff { column, diff } => {
                if at.is_some() {
                    return Err(crate::Error::Config(format!(
                        "{}.{column} is edited in place: the update must leave it unset",
                        T::NAME
                    )));
                }
                lowered.push(diff);
            }
        }
    }
    Ok((cells, lowered))
}

impl Jazz {
    /// Updates the row with this id, as [`Jazz::update`], with `edits`
    /// applied in place: text columns as edits of the values their writer
    /// saw, as [`Jazz::edit_text`] does, and splices and json sets, as
    /// [`Jazz::splice`] and [`Jazz::json_set`] do. TS's
    /// `update(table, id, data, { editedFrom, applyDiffs })`. The sets and
    /// the edits commit as one row update; a failing edit leaves the row
    /// unchanged.
    pub async fn update_edited<U: Update>(
        &self,
        id: ObjectId,
        update: U,
        edits: Edits<U::Row>,
    ) -> Result<()> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let cells = update.into_cells(&implementations)?;
            let written = if edits.is_empty() {
                client.update(<U::Row as crate::Table>::NAME, id, cells)?
            } else {
                let (cells, edits) = lower::<U::Row>(cells, edits)?;
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
        edits: Edits<U::Row>,
    ) -> Result<()> {
        let cells = update.into_cells(self.implementations())?;
        let (cells, edits) = lower::<U::Row>(cells, edits)?;
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
        edits: Edits<U::Row>,
    ) -> Result<()> {
        self.on(move |tx| async move { tx.update_edited(id, update, edits) })
            .await
    }
}
