/// A borrowed SQLite cell. TEXT retains its bytes, including embedded NULs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cell<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

impl<'a> Cell<'a> {
    pub fn as_i64(self) -> Result<i64, CellError> {
        match self {
            Self::Integer(value) => Ok(value),
            _ => Err(CellError("expected an INTEGER cell")),
        }
    }
    pub fn as_blob(self) -> Result<&'a [u8], CellError> {
        match self {
            Self::Blob(value) => Ok(value),
            _ => Err(CellError("expected a BLOB cell")),
        }
    }
}

/// A cell could not be decoded as the requested Rust type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellError(pub &'static str);

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for CellError {}

pub trait FromCell: Sized {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError>;
}
impl FromCell for i64 {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        cell.as_i64()
    }
}
impl FromCell for f64 {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Real(value) => Ok(value),
            Cell::Integer(value) => Ok(value as f64),
            _ => Err(CellError("expected a REAL or INTEGER cell")),
        }
    }
}
macro_rules! integer_cells {
    ($($ty:ty),* $(,)?) => {$ (
        impl FromCell for $ty {
            fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
                i64::from_cell(cell)?.try_into().map_err(|_| CellError("INTEGER is out of range"))
            }
        }
    )*};
}
integer_cells!(i8, i16, i32, isize, u8, u16, u32, u64, usize);

impl FromCell for bool {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        i64::from_cell(cell).map(|value| value != 0)
    }
}
impl FromCell for String {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Text(value) => std::str::from_utf8(value)
                .map(str::to_owned)
                .map_err(|_| CellError("TEXT is not valid UTF-8")),
            _ => Err(CellError("expected a TEXT cell")),
        }
    }
}
impl FromCell for Vec<u8> {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Blob(value) => Ok(value.to_vec()),
            _ => Err(CellError("expected a BLOB cell")),
        }
    }
}
impl<T: FromCell> FromCell for Option<T> {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        match cell {
            Cell::Null => Ok(None),
            other => T::from_cell(other).map(Some),
        }
    }
}

/// Borrow a bound value without converting it through a native SQLite type.
pub trait ToSql {
    fn as_cell(&self) -> Cell<'_>;
}
impl ToSql for Cell<'_> {
    fn as_cell(&self) -> Cell<'_> {
        *self
    }
}
impl<T: ToSql + ?Sized> ToSql for &T {
    fn as_cell(&self) -> Cell<'_> {
        T::as_cell(*self)
    }
}
impl<T: ToSql> ToSql for Option<T> {
    fn as_cell(&self) -> Cell<'_> {
        self.as_ref().map_or(Cell::Null, ToSql::as_cell)
    }
}
macro_rules! integer_params {
    ($($ty:ty),*) => { $(impl ToSql for $ty {
        fn as_cell(&self) -> Cell<'_> { Cell::Integer(*self as i64) }
    })* };
}
integer_params!(i8, i16, i32, i64, u8, u16, u32, bool);
impl ToSql for f64 {
    fn as_cell(&self) -> Cell<'_> {
        Cell::Real(*self)
    }
}
impl ToSql for str {
    fn as_cell(&self) -> Cell<'_> {
        Cell::Text(self.as_bytes())
    }
}
impl ToSql for String {
    fn as_cell(&self) -> Cell<'_> {
        Cell::Text(self.as_bytes())
    }
}
impl ToSql for [u8] {
    fn as_cell(&self) -> Cell<'_> {
        Cell::Blob(self)
    }
}
impl<const N: usize> ToSql for [u8; N] {
    fn as_cell(&self) -> Cell<'_> {
        Cell::Blob(self)
    }
}
impl ToSql for Vec<u8> {
    fn as_cell(&self) -> Cell<'_> {
        Cell::Blob(self)
    }
}

/// Bind a heterogeneous list without allocating or copying its values.
#[macro_export]
macro_rules! params {
    ($($value:expr),* $(,)?) => { &[$(&$value as &dyn $crate::storage::ToSql),*] as &[&dyn $crate::storage::ToSql] };
}

impl ToSql for super::Value {
    fn as_cell(&self) -> Cell<'_> {
        self.as_value_ref()
    }
}

impl FromCell for super::Value {
    fn from_cell(cell: Cell<'_>) -> Result<Self, CellError> {
        Ok(cell.into())
    }
}

#[cfg(feature = "sqlite")]
impl<'a> From<rusqlite::types::ValueRef<'a>> for Cell<'a> {
    fn from(value: rusqlite::types::ValueRef<'a>) -> Self {
        match value {
            rusqlite::types::ValueRef::Null => Self::Null,
            rusqlite::types::ValueRef::Integer(v) => Self::Integer(v),
            rusqlite::types::ValueRef::Real(v) => Self::Real(v),
            rusqlite::types::ValueRef::Text(v) => Self::Text(v),
            rusqlite::types::ValueRef::Blob(v) => Self::Blob(v),
        }
    }
}

#[cfg(feature = "sqlite")]
impl<'a> From<Cell<'a>> for rusqlite::types::ValueRef<'a> {
    fn from(value: Cell<'a>) -> Self {
        match value {
            Cell::Null => Self::Null,
            Cell::Integer(v) => Self::Integer(v),
            Cell::Real(v) => Self::Real(v),
            Cell::Text(v) => Self::Text(v),
            Cell::Blob(v) => Self::Blob(v),
        }
    }
}

#[cfg(feature = "sqlite")]
impl rusqlite::ToSql for Cell<'_> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Borrowed((*self).into()))
    }
}
