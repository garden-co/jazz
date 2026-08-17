// This helper is shared temporarily with Jazz's integration suite. The
// dedicated testkit extraction will give it a package-owned home.
#[path = "../../../jazz/tests/support/permissions.rs"]
mod permissions;

pub use permissions::publish_allow_all_permissions;
