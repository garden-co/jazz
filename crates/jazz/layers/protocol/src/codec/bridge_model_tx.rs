//! Temporary postcard bridges for types the `model-tx` codecs reach but other
//! parts own (query shapes, leaf ids and times, Groove values). Deleted when
//! the parts are merged.

use super::{CodecError, WireCodec};

macro_rules! postcard_bridge {
    ($($ty:ty),+ $(,)?) => {$(
        impl WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                buf.extend(postcard::to_allocvec(self).unwrap())
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                let (value, rest) = postcard::take_from_bytes(&data[*offset..]).map_err(|e| {
                    CodecError::DecodeError {
                        message: e.to_string(),
                    }
                })?;
                *offset = data.len() - rest.len();
                Ok(value)
            }
        }
    )+};
}

postcard_bridge!(
    jazz_model::query::Query,
    jazz_model::query::ShapeId,
    jazz_model::query::BindingId,
    jazz_types::ids::AuthorSubject,
    jazz_types::ids::NodeUuid,
    jazz_types::ids::RowUuid,
    jazz_types::ids::PhysicalTableId,
    jazz_types::ids::SchemaVersionId,
    jazz_types::time::TxTime,
    jazz_types::time::GlobalTime,
    groove::records::Value,
);
