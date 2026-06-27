//! Lens vocabulary.
//!
//! The old schema-manager runtime, environment branch composition, catalogue
//! rehydrate, and auto-lens engine have been removed. What remains here is the
//! lens vocabulary still shared by server routes and client setup.

pub mod lens;

pub use lens::{Direction, Lens, LensOp, LensTransform};
