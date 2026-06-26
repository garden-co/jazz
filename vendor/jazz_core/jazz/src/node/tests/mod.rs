//! NodeState integration and regression tests. This module owns test-only wiring for
//! the storage-backed node layer; production node behavior lives in sibling
//! modules, and independent expected semantics live in [`crate::oracle`].

use super::*;

mod harness;
