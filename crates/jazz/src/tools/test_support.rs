//! Policy fixtures shared by Jazz's tests.

#[cfg(any(test, feature = "testing"))]
pub use crate::model::test_support::{AllowAll, allow_all_policies};
