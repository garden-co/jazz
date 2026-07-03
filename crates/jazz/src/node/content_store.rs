//! Append-only large-value content storage over Jazz's direct groove record stores.

use groove::db::Database;
use groove::records::Value;
use groove::storage::OrderedKvStorage;

use crate::ids::{AuthorId, RowUuid};
use crate::schema::{CONTENT_CHECKPOINTS_STORE, CONTENT_EXTENTS_STORE, CONTENT_META_STORE};
use crate::tx::TxId;

use super::Error;

/// Byte range in a writer's append-only stream for one `(row, column)`.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct Extent {
    /// Writer that owns the stream.
    pub writer: AuthorId,
    /// Row addressed by the stream.
    pub row: RowUuid,
    /// Column addressed by the stream.
    pub column: String,
    /// Byte offset within the stream.
    pub offset: u64,
    /// Number of bytes in the extent.
    pub len: u64,
}

/// Pure-storage API for Jazz large-value byte streams.
pub struct ContentStore<'a, S> {
    database: &'a Database<S>,
}

impl<'a, S> ContentStore<'a, S>
where
    S: OrderedKvStorage,
{
    pub(super) fn new(database: &'a Database<S>) -> Self {
        Self { database }
    }

    /// Append bytes to the end of a `(writer, row, column)` stream.
    pub fn append(
        &self,
        writer: AuthorId,
        row: RowUuid,
        column: &str,
        bytes: &[u8],
    ) -> Result<Extent, Error> {
        let meta = self.database.direct_record_store(CONTENT_META_STORE)?;
        let meta_key = stream_key(writer, row, column);
        let offset = match meta.get(&meta_key)? {
            Some(value) => match value.get("offset")? {
                Value::U64(offset) => offset,
                _ => return Err(Error::InvalidStoredValue("invalid content offset")),
            },
            None => 0,
        };
        let len = u64::try_from(bytes.len())
            .map_err(|_| Error::InvalidStoredValue("content too large"))?;
        let extent = Extent {
            writer,
            row,
            column: column.to_owned(),
            offset,
            len,
        };
        if len > 0 {
            self.database
                .direct_record_store(CONTENT_EXTENTS_STORE)?
                .set(
                    &extent_key(writer, row, column, offset),
                    &[Value::Bytes(bytes.to_vec())],
                )?;
        }
        let next_offset = offset
            .checked_add(len)
            .ok_or(Error::InvalidStoredValue("content stream offset overflow"))?;
        meta.set(&meta_key, &[Value::U64(next_offset)])?;
        Ok(extent)
    }

    /// Store bytes at an already-named extent.
    ///
    /// This is the receiving-side counterpart to [`Self::append`]: extents are
    /// canonical names on the wire, so the receiver must materialize bytes at
    /// that exact range rather than appending to a local stream tail.
    pub fn put_extent(&self, extent: &Extent, bytes: &[u8]) -> Result<(), Error> {
        let len = u64::try_from(bytes.len())
            .map_err(|_| Error::InvalidStoredValue("content too large"))?;
        if len != extent.len {
            return Err(Error::InvalidStoredValue(
                "content extent payload length mismatch",
            ));
        }
        if self.contains(extent)? {
            if self.read(extent)? == bytes {
                return Ok(());
            }
            return Err(Error::InvalidStoredValue("conflicting content extent"));
        }

        if len > 0 {
            self.database
                .direct_record_store(CONTENT_EXTENTS_STORE)?
                .set(
                    &extent_key(extent.writer, extent.row, &extent.column, extent.offset),
                    &[Value::Bytes(bytes.to_vec())],
                )?;
        }
        let meta = self.database.direct_record_store(CONTENT_META_STORE)?;
        let meta_key = stream_key(extent.writer, extent.row, &extent.column);
        let current = match meta.get(&meta_key)? {
            Some(value) => match value.get("offset")? {
                Value::U64(offset) => offset,
                _ => return Err(Error::InvalidStoredValue("invalid content offset")),
            },
            None => 0,
        };
        let end = extent
            .offset
            .checked_add(extent.len)
            .ok_or(Error::InvalidStoredValue("content stream offset overflow"))?;
        if end > current {
            meta.set(&meta_key, &[Value::U64(end)])?;
        }
        Ok(())
    }

    /// Return true when the exact extent can be read locally.
    pub fn contains(&self, extent: &Extent) -> Result<bool, Error> {
        match self.read(extent) {
            Ok(_) => Ok(true),
            Err(Error::MissingContentExtent(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Read an extent back from its stream bytes.
    pub fn read(&self, extent: &Extent) -> Result<Vec<u8>, Error> {
        if extent.len == 0 {
            return Ok(Vec::new());
        }
        let extents = self.database.direct_record_store(CONTENT_EXTENTS_STORE)?;
        let end = extent
            .offset
            .checked_add(extent.len)
            .ok_or(Error::InvalidStoredValue("content extent offset overflow"))?;
        let mut out = Vec::with_capacity(
            usize::try_from(extent.len)
                .map_err(|_| Error::InvalidStoredValue("content extent too large"))?,
        );
        for entry in extents.range_entries(
            &extent_key(extent.writer, extent.row, &extent.column, extent.offset),
            &extent_key(extent.writer, extent.row, &extent.column, end),
        )? {
            let offset = match entry.key.get(3) {
                Some(Value::U64(offset)) => *offset,
                _ => return Err(Error::InvalidStoredValue("invalid content extent offset")),
            };
            if offset != extent.offset + u64::try_from(out.len()).unwrap_or(u64::MAX) {
                return Err(Error::MissingContentExtent(extent.clone()));
            }
            match entry.value.get("bytes")? {
                Value::Bytes(bytes) => out.extend_from_slice(&bytes),
                _ => return Err(Error::InvalidStoredValue("invalid content extent bytes")),
            }
            if u64::try_from(out.len()).unwrap_or(u64::MAX) > extent.len {
                return Err(Error::InvalidStoredValue("content extent over-read"));
            }
        }
        if u64::try_from(out.len()).unwrap_or(u64::MAX) != extent.len {
            return Err(Error::MissingContentExtent(extent.clone()));
        }
        Ok(out)
    }

    /// Store a local materialized large-value checkpoint for one version.
    pub fn put_checkpoint(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        version: TxId,
        bytes: &[u8],
    ) -> Result<(), Error> {
        self.database
            .direct_record_store(CONTENT_CHECKPOINTS_STORE)?
            .set(
                &checkpoint_key(table, row, column, version),
                &[Value::Bytes(bytes.to_vec())],
            )?;
        Ok(())
    }

    /// Read a local materialized large-value checkpoint for one version.
    pub fn checkpoint(
        &self,
        table: &str,
        row: RowUuid,
        column: &str,
        version: TxId,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.database
            .direct_record_store(CONTENT_CHECKPOINTS_STORE)?
            .get(&checkpoint_key(table, row, column, version))?
            .map(|record| match record.get("bytes") {
                Ok(Value::Bytes(bytes)) => Ok(bytes),
                Ok(_) => Err(Error::InvalidStoredValue(
                    "invalid content checkpoint bytes",
                )),
                Err(err) => Err(err.into()),
            })
            .transpose()
    }
}

fn stream_key(writer: AuthorId, row: RowUuid, column: &str) -> Vec<Value> {
    vec![
        Value::Uuid(writer.0),
        Value::Uuid(row.0),
        Value::String(column.to_owned()),
    ]
}

fn extent_key(writer: AuthorId, row: RowUuid, column: &str, offset: u64) -> Vec<Value> {
    let mut key = stream_key(writer, row, column);
    key.push(Value::U64(offset));
    key
}

fn checkpoint_key(table: &str, row: RowUuid, column: &str, version: TxId) -> Vec<Value> {
    vec![
        Value::String(table.to_owned()),
        Value::Uuid(row.0),
        Value::String(column.to_owned()),
        Value::U64(version.time.0),
        Value::Uuid(version.node.0),
    ]
}
