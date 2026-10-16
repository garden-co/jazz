//! TEMPORARY postcard bridges for types the protocol-b codecs use but other
//! parts own. Deleted when the parts are merged.

use super::{CodecError, WireCodec, record_encode_error};
use crate::query::RelationQuery;

fn bridge_put<T: serde::Serialize>(value: &T, buf: &mut Vec<u8>) {
    match postcard::to_allocvec(value) {
        Ok(bytes) => buf.extend(bytes),
        Err(error) => record_encode_error(CodecError::Encode {
            message: error.to_string(),
        }),
    }
}

fn bridge_get<T: serde::de::DeserializeOwned>(
    data: &[u8],
    offset: &mut usize,
) -> Result<T, CodecError> {
    let (value, rest) =
        postcard::take_from_bytes(&data[*offset..]).map_err(|error| CodecError::DecodeError {
            message: error.to_string(),
        })?;
    *offset = data.len() - rest.len();
    Ok(value)
}

macro_rules! postcard_bridge {
    ($($ty:ty),+ $(,)?) => {$(
        impl WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                bridge_put(self, buf)
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                bridge_get(data, offset)
            }
        }
    )+};
}

postcard_bridge!(
    crate::ids::SchemaVersionId,
    crate::ids::MigrationLensId,
    crate::ids::SchemaLineagePublicationId,
    crate::ids::RowUuid,
    crate::ids::AuthorSubject,
    crate::ids::GlobalPhysicalTableId,
    crate::ids::GlobalPhysicalColumnId,
    crate::ids::GlobalPhysicalEnumVariantId,
    crate::time::GlobalTime,
    crate::tx::TxId,
    crate::tx::DurabilityTier,
    crate::object::OutputOccurrenceId,
    crate::object::ResultKey,
    crate::query::Query,
    crate::query::ShapeId,
    crate::query::BindingId,
    crate::protocol::SubscriptionKey,
    crate::protocol::ReservedWireMessage,
    crate::protocol::BranchSelector,
    crate::protocol::BranchViewBase,
    crate::protocol::SnapshotRef,
    groove::Intern<String>,
    groove::records::Value,
);

/// `RelationQuery` crosses postcard only through
/// `relation_query_wire::{serialize,deserialize}_required`, so its sync-wire
/// codec is that typed wire form.
#[derive(serde::Serialize, serde::Deserialize)]
struct RequiredRelation(
    #[serde(
        serialize_with = "crate::query::relation_query_wire::serialize_required",
        deserialize_with = "crate::query::relation_query_wire::deserialize_required"
    )]
    RelationQuery,
);

impl WireCodec for RelationQuery {
    fn put(&self, buf: &mut Vec<u8>) {
        bridge_put(&RequiredRelation(self.clone()), buf)
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        bridge_get::<RequiredRelation>(data, offset).map(|wire| wire.0)
    }
}
