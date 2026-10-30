//! Canonical, whole-input postcard decoding shared by every wire carrier.

use postcard::take_from_bytes;
use serde::{Deserialize, Serialize};

/// Decode one canonical postcard value only when it consumes the complete input.
///
/// Postcard's `from_bytes` intentionally leaves an unread suffix invisible to
/// callers. A Jazz wire frame and its semantic payload are each exactly one
/// value, so accepting such a suffix would give one physical frame two
/// interpretations at adjacent protocol seams. Postcard itself also accepts
/// alternate overlong varint spellings, while the frozen wire contract admits
/// only the encoder's shortest spelling.
///
/// This is the shared ownership boundary for protocol carriers that contain
/// exactly one postcard value, including WebSocket frame batches. Callers must
/// apply their carrier-specific size and cardinality limits separately.
pub fn decode_postcard_exact<'a, T>(bytes: &'a [u8]) -> Result<T, postcard::Error>
where
    T: Deserialize<'a> + Serialize,
{
    let (value, remainder) = take_from_bytes(bytes)?;
    if !remainder.is_empty() || !encodes_exactly(&value, bytes) {
        Err(postcard::Error::DeserializeBadEncoding)
    } else {
        Ok(value)
    }
}

/// Whether the canonical postcard encoding of `value` is exactly `expected`.
///
/// Equivalent to `to_allocvec(value)? == expected`, but compares while
/// serializing: it allocates nothing and stops at the first differing byte.
pub fn encodes_exactly<T: Serialize + ?Sized>(value: &T, expected: &[u8]) -> bool {
    postcard::serialize_with_flavor(value, CanonicalBytesMatch { expected })
        .is_ok_and(|remaining: &[u8]| remaining.is_empty())
}

/// Postcard output flavor that consumes `expected` instead of writing bytes.
struct CanonicalBytesMatch<'a> {
    expected: &'a [u8],
}

impl<'a> postcard::ser_flavors::Flavor for CanonicalBytesMatch<'a> {
    /// The expected bytes the encoding did not reach.
    type Output = &'a [u8];

    fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
        self.try_extend(&[byte])
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        match self.expected.split_at_checked(bytes.len()) {
            Some((head, tail)) if head == bytes => {
                self.expected = tail;
                Ok(())
            }
            _ => Err(postcard::Error::SerializeBufferFull),
        }
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(self.expected)
    }
}
