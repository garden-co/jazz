//! Schema payload encoding and lens vocabulary.
//!
//! The old schema-manager runtime, environment branch composition, catalogue
//! rehydrate, and auto-lens engine have been removed. What remains here is the
//! payload vocabulary still shared by server routes and client setup.

pub mod encoding;
pub mod lens;

pub use encoding::{
    CatalogueEncodingError, decode_lens_transform, decode_permissions, decode_schema,
    encode_lens_transform, encode_permissions, encode_schema,
};
pub use lens::{Direction, Lens, LensOp, LensTransform};
