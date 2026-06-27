// Re-export encoding module from parent for use by sub-modules
pub(crate) use crate::admin_catalogue_row_format;

// Sub-modules
mod branch;
mod descriptor;
mod policy;
mod row;
mod schema;
mod value;

// Re-export all public items from sub-modules
pub use branch::*;
pub use descriptor::*;
pub use policy::*;
pub use row::*;
pub use schema::*;
pub use value::*;

// Import PolicyExpr for use by schema module
pub(crate) use crate::public_api::policy::PolicyExpr;

// Tests module
#[cfg(test)]
mod tests;
