//! TEMPORARY postcard bridges for leaf types (ids, times, Groove values)
//! until their hand-written codecs land. Deleted with the leaves part.

use super::{CodecError, WireCodec, record_encode_error};

macro_rules! postcard_bridge {
    ($($ty:ty),+ $(,)?) => {$(
        impl WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                match postcard::to_allocvec(self) {
                    Ok(bytes) => buf.extend(bytes),
                    Err(error) => record_encode_error(CodecError::Encode { message: error.to_string() }),
                }
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                let (value, rest) = postcard::take_from_bytes(&data[*offset..])
                    .map_err(|e| CodecError::DecodeError { message: e.to_string() })?;
                *offset = data.len() - rest.len();
                Ok(value)
            }
        }
    )+};
}

postcard_bridge!(
    crate::ids::AuthorSubject,
    crate::ids::NodeUuid,
    crate::ids::RowUuid,
    crate::ids::SchemaVersionId,
    crate::ids::PhysicalTableId,
    crate::ids::GlobalPhysicalTableId,
    crate::ids::GlobalPhysicalColumnId,
    crate::ids::GlobalPhysicalEnumVariantId,
    crate::ids::MigrationLensId,
    crate::ids::SchemaLineagePublicationId,
    crate::time::GlobalTime,
    crate::time::TxTime,
    crate::object::OutputOccurrenceId,
    crate::object::ResultKey,
    groove::Intern<String>,
    groove::records::Value,
    groove::large_values::Locator,
    groove::large_values::LargeValueRef,
    groove::large_values::StagedChunk,
    groove::large_values::NodeRef,
);
