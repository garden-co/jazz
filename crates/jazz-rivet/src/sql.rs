use anyhow::{Result, ensure};
use jazz_sql::Param;
use rivetkit::{BindParam, ColumnValue, SqliteDb};

pub(crate) async fn parameter_limit(db: &SqliteDb) -> Result<usize> {
    // This worker does not change SQLITE_LIMIT_VARIABLE_NUMBER. Read the
    // engine's compiled bound once rather than assume a SQLite version's
    // default. The SDK guard only changes the value-length limit.
    let limits = db.query(
        "SELECT compile_options FROM pragma_compile_options WHERE compile_options LIKE 'MAX_VARIABLE_NUMBER=%' LIMIT 1",
        None,
    ).await?;
    let parameter_limit = match limits.rows.first().and_then(|row| row.first()) {
        Some(ColumnValue::Text(option)) => option
            .strip_prefix("MAX_VARIABLE_NUMBER=")
            .ok_or_else(|| anyhow::anyhow!("invalid SQLite parameter limit"))?
            .parse::<usize>()?,
        _ => anyhow::bail!("Rivet SQLite did not report its parameter limit"),
    };
    ensure!(parameter_limit > 0, "invalid SQLite parameter limit");
    Ok(parameter_limit)
}

pub(crate) fn bind(params: &[Param<'_>]) -> Result<Option<Vec<BindParam>>> {
    Ok(Some(
        params
            .iter()
            .map(|param| {
                Ok(match param {
                    Param::Null => BindParam::Null,
                    Param::Integer(value) => BindParam::Integer(*value),
                    Param::Real(value) => BindParam::Float(*value),
                    Param::Blob(value) => BindParam::Blob(value.to_vec()),
                    Param::Text(value) => BindParam::Text((*value).into()),
                    Param::TextBytes(value) => BindParam::Text(
                        std::str::from_utf8(value)
                            .map_err(|_| {
                                jazz_sql::CellError(
                                    "Binding non-UTF-8 TEXT is not supported by Rivet SQLite",
                                )
                            })?
                            .into(),
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}
