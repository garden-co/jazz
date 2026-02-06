//! Create command implementations.

use groove::schema_manager::AppId;

/// Create a new application.
///
/// If a name is provided, generates a deterministic ID from the name.
/// Otherwise, generates a random ID.
pub fn app(name: Option<String>) {
    let app_id = match name {
        Some(n) => AppId::from_name(&n),
        None => AppId::random(),
    };

    println!("{}", app_id);
}
