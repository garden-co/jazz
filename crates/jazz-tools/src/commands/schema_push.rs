//! Schema push command implementation — delegates to `jazz_tools::schema_catalogue::push`.

/// Run the schema push command.
pub async fn run(
    server_url: &str,
    app_id: &str,
    env: &str,
    user_branch: &str,
    admin_secret: &str,
    schema_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    jazz_tools::schema_catalogue::push(
        server_url,
        app_id,
        env,
        user_branch,
        admin_secret,
        schema_dir,
    )
    .await
}
