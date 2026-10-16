//! TEMPORARY postcard bridges for types the protocol-a codecs reference but
//! other parts own. Deleted when the parts are merged.

use super::{CodecError, WireCodec};

macro_rules! postcard_bridge {
    ($($ty:ty),+ $(,)?) => {$(
        impl WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                buf.extend(postcard::to_allocvec(self).unwrap())
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

// leaves
postcard_bridge!(
    crate::ids::AuthorSubject,
    crate::ids::NodeUuid,
    crate::ids::RowUuid,
    crate::ids::SchemaVersionId,
    crate::ids::GlobalPhysicalTableId,
    crate::time::GlobalTime,
    crate::time::TxTime,
    groove::Intern<String>,
    groove::records::Value,
    groove::large_values::Locator,
    groove::large_values::LargeValueRef,
    groove::large_values::StagedChunk,
    groove::large_values::NodeRef,
);

// model-tx / model-query
postcard_bridge!(
    crate::tx::Transaction,
    crate::tx::TxId,
    crate::tx::Fate,
    crate::tx::DurabilityTier,
    crate::query::ShapeId,
    crate::query::BindingId,
    crate::protocol::BranchKey,
);

// protocol-b
postcard_bridge!(
    crate::protocol::ShapeAst,
    crate::protocol::RegisterShapeOptions,
    crate::protocol::Subscribe,
    crate::protocol::SubscribeRejectReason,
    crate::protocol::SchemaVersion,
    crate::protocol::SchemaLineagePublication,
    crate::protocol::MigrationLens,
    crate::protocol::CatalogueAck,
    crate::protocol::DelegatedSessionBinding,
    crate::protocol::SupportingRow,
    crate::protocol::PhysicalIdentityManifest,
    crate::protocol::CurrentWriteSchema,
    crate::protocol::ReadViewKey,
);
