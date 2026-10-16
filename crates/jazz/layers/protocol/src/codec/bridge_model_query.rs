//! Temporary postcard bridges for types owned by other codec parts, so the
//! model-query codecs build on their own. Deleted when the parts are merged.

use crate::codec::{CodecError, WireCodec};

macro_rules! postcard_bridge {
    ($($ty:ty),+) => {$(
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

postcard_bridge!(groove::records::Value);
