//! Temporary bridges for identifiers owned by part "leaves", spelled through
//! postcard until their hand-written codecs land.

use super::{CodecError, WireCodec};

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

postcard_bridge!(crate::ids::AuthorSubject, crate::ids::NodeUuid);
