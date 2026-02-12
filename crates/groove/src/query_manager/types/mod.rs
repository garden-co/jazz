// Re-export encoding module from parent for use by sub-modules
pub(crate) use super::encoding;

// Sub-modules
pub mod branch;
pub mod descriptor;
pub mod policy;
pub mod row;
pub mod schema;
pub mod tuple;
pub mod value;

// Re-export all public items from sub-modules
pub use branch::*;
pub use descriptor::*;
pub use policy::*;
pub use row::*;
pub use schema::*;
pub use tuple::*;
pub use value::*;

// Import PolicyExpr for use by schema module
pub(crate) use crate::query_manager::policy::PolicyExpr;

// Tests module
#[cfg(test)]
mod tests;
