/// Statement execution or row decoding failed.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("{message}")]
    Backend { code: Option<i32>, message: String },
    #[error("Query returned no rows")]
    QueryReturnedNoRows,
    #[error("invalid column {index} ({name}): {reason}")]
    InvalidColumnType {
        index: usize,
        name: String,
        reason: super::CellError,
    },
    #[error("Invalid column index: {0}")]
    InvalidColumnIndex(usize),
    #[error("Invalid column name: {0}")]
    InvalidColumnName(String),
    #[error("Wrong number of parameters passed to query. Got {0}, needed {1}")]
    InvalidParameterCount(usize, usize),
    #[error("could not bind parameter: {0}")]
    ToSqlConversionFailure(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub type SqlResult<T> = Result<T, SqlError>;

#[cfg(feature = "sqlite")]
impl From<rusqlite::Error> for SqlError {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(error, rusqlite::Error::QueryReturnedNoRows) {
            return Self::QueryReturnedNoRows;
        }
        Self::Backend {
            code: error.sqlite_error().map(|error| error.extended_code),
            message: error.to_string(),
        }
    }
}

pub trait OptionalExtension<T> {
    fn optional(self) -> SqlResult<Option<T>>;
}

impl<T> OptionalExtension<T> for SqlResult<T> {
    fn optional(self) -> SqlResult<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(SqlError::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error),
        }
    }
}
