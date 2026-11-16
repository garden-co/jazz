//! What generated bindings implement for each table.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use jazz::tools::{ObjectId, QueryResultField, Value};

use crate::error::{Error, Result};
use crate::include::Row;
use crate::value::ColumnValue;

/// A table's row type, generated from `jazz.app.json`.
pub trait Table: Sized + Send + 'static {
    /// The table's name in `jazz.app.json`.
    const NAME: &'static str;
    /// The table's stored columns, as `jazz.app.json` names them.
    const COLUMNS: &'static [&'static str];
    /// Values for a new row.
    type New: Insert<Row = Self>;
    /// A partial update of a row.
    type Update: Update<Row = Self>;

    /// Reads a row from its fields, by column name.
    fn decode(row: &mut Fields<'_>) -> Result<Self>;

    fn id(&self) -> ObjectId;
}

/// Values for a new row of [`Insert::Row`].
pub trait Insert: Send + 'static {
    type Row: Table;
    /// Cells by column name. Every column the table declares and the
    /// insert leaves out must have a default.
    fn into_cells(self, implementations: &Implementations) -> Result<HashMap<String, Value>>;
}

/// A partial update of a row of [`Update::Row`].
pub trait Update: Send + 'static {
    type Row: Table;
    /// The cells it changes, by column name.
    fn into_cells(self, implementations: &Implementations) -> Result<Vec<(String, Value)>>;
}

/// A row's fields, as a query delivered them.
pub struct Fields<'a> {
    table: &'static str,
    id: ObjectId,
    fields: Vec<QueryResultField>,
    /// Where the next column usually is: bindings take them in the order
    /// the query delivers them.
    next: usize,
    implementations: &'a Implementations,
}

impl<'a> Fields<'a> {
    pub(crate) fn new(
        table: &'static str,
        id: ObjectId,
        fields: Vec<QueryResultField>,
        implementations: &'a Implementations,
    ) -> Self {
        Self {
            table,
            id,
            fields,
            next: 0,
            implementations,
        }
    }

    pub fn id(&self) -> ObjectId {
        self.id
    }

    /// The app's implementations of its computed columns and transforms.
    pub fn implementations(&self) -> &'a Implementations {
        self.implementations
    }

    /// Takes the column `name`'s value.
    pub fn take<T: ColumnValue>(&mut self, name: &str) -> Result<T> {
        let at = match self.fields.get(self.next) {
            Some(field) if field.name == name => self.next,
            _ => self
                .fields
                .iter()
                .position(|field| field.name == name)
                .ok_or_else(|| self.error(format!("no column {name}")))?,
        };
        self.next = at + 1;
        let value = std::mem::replace(&mut self.fields[at].value, Value::Null);
        T::from_value(value).map_err(|value| {
            self.error(format!(
                "column {name} holds {value:?}, not a {}",
                std::any::type_name::<T>()
            ))
        })
    }

    /// Takes the next column's value: partial rows hold their values in the
    /// order they were selected.
    pub(crate) fn take_next<T: ColumnValue>(&mut self) -> Result<T> {
        let Some(field) = self.fields.get_mut(self.next) else {
            return Err(self.error(format!("no column at {}", self.next)));
        };
        self.next += 1;
        let name = field.name.clone();
        let value = std::mem::replace(&mut field.value, Value::Null);
        T::from_value(value).map_err(|value| {
            self.error(format!(
                "column {name} holds {value:?}, not a {}",
                std::any::type_name::<T>()
            ))
        })
    }

    /// Takes the field `name`'s value as it is: an include's rows.
    pub(crate) fn take_raw(&mut self, name: &str) -> Result<Value> {
        let field = self
            .fields
            .iter_mut()
            .find(|field| field.name == name)
            .ok_or_else(|| Error::Decode {
                table: self.table,
                message: format!("no include {name}"),
            })?;
        Ok(std::mem::replace(&mut field.value, Value::Null))
    }

    fn error(&self, message: String) -> Error {
        Error::Decode {
            table: self.table,
            message,
        }
    }
}

pub(crate) fn decode<R: Row>(
    id: ObjectId,
    fields: Vec<QueryResultField>,
    implementations: &Implementations,
) -> Result<R> {
    R::decode_fields(&mut Fields::new(
        <R::Table as Table>::NAME,
        id,
        fields,
        implementations,
    ))
}

/// What the app's code implements of what `jazz.app.json` declares: its
/// computed columns and transforms. Generated bindings declare them as a
/// `Computed` trait and their `open` takes an implementation of it.
#[derive(Clone, Default)]
pub struct Implementations(Option<Arc<dyn Any + Send + Sync>>);

impl Implementations {
    pub fn new<T: Any + Send + Sync>(implementations: T) -> Self {
        Self(Some(Arc::new(implementations)))
    }

    /// The implementations, as the type generated bindings stored them.
    pub fn get<T: Any>(&self) -> Result<&T> {
        self.0
            .as_deref()
            .and_then(|any| (any as &dyn Any).downcast_ref())
            .ok_or_else(|| {
                Error::Config(format!(
                    "the app was opened without its {}",
                    std::any::type_name::<T>()
                ))
            })
    }
}

impl std::fmt::Debug for Implementations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "Implementations(..)"
        } else {
            "Implementations(none)"
        })
    }
}
