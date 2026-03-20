//! Schema catalogue push logic.
//!
//! Exposes the schema-push logic as a library function usable from NAPI and other bindings.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::jazz_transport::SyncBatchRequest;
use crate::query_manager::types::SchemaHash;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::{
    AppId, Direction, Lens, MigrationFileInfo, SchemaDirectory, SchemaManager,
    parse_migration_filename,
};
use crate::storage::MemoryStorage;
use crate::sync_manager::{ClientId, Destination, OutboxEntry, ServerId, SyncManager};
use reqwest::Client;
use reqwest::header::CONTENT_TYPE;

struct SyncServerClient {
    http_client: Client,
    base_url: String,
    route_prefix: String,
    admin_secret: String,
}

impl SyncServerClient {
    async fn connect(
        server_url: &str,
        admin_secret: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let http_client = Client::new();
        let (base_url, route_prefix) = split_base_url(server_url)?;

        let health_url = format!("{base_url}/health");
        http_client
            .get(health_url)
            .send()
            .await?
            .error_for_status()?;

        Ok(Self {
            http_client,
            base_url,
            route_prefix,
            admin_secret: admin_secret.to_string(),
        })
    }

    async fn push_sync(
        &self,
        payload: crate::sync_manager::SyncPayload,
        client_id: ClientId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let request = SyncBatchRequest {
            payloads: vec![payload],
            client_id,
        };
        let sync_url = format!("{}{}/sync", self.base_url, self.route_prefix);
        self.http_client
            .post(sync_url)
            .header(CONTENT_TYPE, "application/json")
            .header("X-Jazz-Admin-Secret", &self.admin_secret)
            .json(&request)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

fn split_base_url(input: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let parsed = reqwest::Url::parse(input)?;

    let mut origin = parsed.clone();
    origin.set_path("");
    origin.set_query(None);
    origin.set_fragment(None);

    let base_url = origin.as_str().trim_end_matches('/').to_string();
    let route_prefix = normalize_route_prefix(parsed.path());

    Ok((base_url, route_prefix))
}

fn normalize_route_prefix(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return String::new();
    }

    let trimmed = trimmed.trim_end_matches('/');
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn build_runtime(
    schema_manager: SchemaManager,
    storage: MemoryStorage,
    connection: Arc<SyncServerClient>,
    client_id: ClientId,
    in_flight_pushes: Arc<AtomicUsize>,
    push_errors: Arc<Mutex<Vec<String>>>,
) -> TokioRuntime<MemoryStorage> {
    TokioRuntime::new(schema_manager, storage, move |entry: OutboxEntry| {
        let OutboxEntry {
            destination,
            payload,
        } = entry;
        if let Destination::Server(_) = destination {
            in_flight_pushes.fetch_add(1, Ordering::AcqRel);
            let connection = connection.clone();
            let push_errors = push_errors.clone();
            let in_flight_pushes = in_flight_pushes.clone();
            tokio::spawn(async move {
                if let Err(error) = connection.push_sync(payload, client_id).await
                    && let Ok(mut errors) = push_errors.lock()
                {
                    errors.push(error.to_string());
                }
                in_flight_pushes.fetch_sub(1, Ordering::AcqRel);
            });
        }
    })
}

fn collect_forward_migration_files(
    schema_directory: &SchemaDirectory,
) -> Result<Vec<MigrationFileInfo>, Box<dyn std::error::Error>> {
    let mut forward_migrations = Vec::new();
    for entry in fs::read_dir(schema_directory.path())? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let filename = entry.file_name();
        let filename = filename.to_string_lossy();
        let Some(info) = parse_migration_filename(&filename) else {
            continue;
        };

        if info.direction == Some(Direction::Forward) {
            forward_migrations.push(info);
        }
    }

    forward_migrations.sort_by(|left, right| {
        (
            left.from_version,
            left.to_version,
            left.from_hash.as_str(),
            left.to_hash.as_str(),
        )
            .cmp(&(
                right.from_version,
                right.to_version,
                right.from_hash.as_str(),
                right.to_hash.as_str(),
            ))
    });

    Ok(forward_migrations)
}

async fn wait_for_in_flight_pushes(in_flight_pushes: &Arc<AtomicUsize>) {
    while in_flight_pushes.load(Ordering::Acquire) > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Push schema catalogue objects to a sync server.
pub async fn push(
    server_url: &str,
    app_id: &str,
    env: &str,
    user_branch: &str,
    admin_secret: &str,
    schema_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let app_id = AppId::from_string(app_id)?;
    let connection = Arc::new(SyncServerClient::connect(server_url, admin_secret).await?);
    let client_id = ClientId::new();
    let in_flight_pushes = Arc::new(AtomicUsize::new(0));
    let push_errors = Arc::new(Mutex::new(Vec::<String>::new()));

    let schema_directory = SchemaDirectory::new(schema_dir);
    let schema_versions = schema_directory.schema_versions()?;
    if schema_versions.is_empty() {
        return Err(format!("No versioned schema files found in {schema_dir}.").into());
    }

    let mut schema_info_by_hash = HashMap::new();
    for schema_info in &schema_versions {
        schema_info_by_hash.insert(schema_info.hash.clone(), schema_info.clone());
    }

    for schema_info in &schema_versions {
        let schema = schema_directory.schema_by_info(schema_info)?;
        let schema_manager =
            SchemaManager::new(SyncManager::new(), schema, app_id, env, user_branch).map_err(
                |error| format!("Failed to initialize schema manager for schema push: {error:?}"),
            )?;
        let runtime = build_runtime(
            schema_manager,
            MemoryStorage::default(),
            connection.clone(),
            client_id,
            in_flight_pushes.clone(),
            push_errors.clone(),
        );

        runtime.persist_schema()?;
        runtime.add_server(ServerId::default())?;
        runtime.flush().await?;
    }

    let forward_migrations = collect_forward_migration_files(&schema_directory)?;
    for migration in &forward_migrations {
        let source_schema_info =
            schema_info_by_hash
                .get(&migration.from_hash)
                .ok_or_else(|| {
                    format!(
                        "Missing source schema file for migration v{}->v{} (hash {}).",
                        migration.from_version, migration.to_version, migration.from_hash
                    )
                })?;
        let target_schema_info = schema_info_by_hash.get(&migration.to_hash).ok_or_else(|| {
            format!(
                "Missing target schema file for migration v{}->v{} (hash {}).",
                migration.from_version, migration.to_version, migration.to_hash
            )
        })?;

        let source_schema = schema_directory.schema_by_info(source_schema_info)?;
        let target_schema = schema_directory.schema_by_info(target_schema_info)?;
        let source_hash = SchemaHash::compute(&source_schema);
        let target_hash = SchemaHash::compute(&target_schema);
        let forward_transform = schema_directory.migration(
            migration.from_version,
            migration.to_version,
            &migration.from_hash,
            &migration.to_hash,
            Direction::Forward,
        )?;
        let backward_transform = if schema_directory.has_migration_sql(
            migration.from_version,
            migration.to_version,
            &migration.from_hash,
            &migration.to_hash,
            Direction::Backward,
        ) {
            schema_directory.migration(
                migration.from_version,
                migration.to_version,
                &migration.from_hash,
                &migration.to_hash,
                Direction::Backward,
            )?
        } else {
            forward_transform.invert()
        };
        let lens = Lens::with_backward(
            source_hash,
            target_hash,
            forward_transform,
            backward_transform,
        );

        let mut storage = MemoryStorage::default();
        let mut schema_manager =
            SchemaManager::new(SyncManager::new(), source_schema, app_id, env, user_branch)
                .map_err(|error| {
                    format!("Failed to initialize schema manager for lens push: {error:?}")
                })?;
        schema_manager.persist_lens(&mut storage, &lens);
        let runtime = build_runtime(
            schema_manager,
            storage,
            connection.clone(),
            client_id,
            in_flight_pushes.clone(),
            push_errors.clone(),
        );

        runtime.add_server(ServerId::default())?;
        runtime.flush().await?;
    }

    wait_for_in_flight_pushes(&in_flight_pushes).await;

    let errors = push_errors.lock().unwrap().clone();
    if !errors.is_empty() {
        return Err(format!(
            "Schema push encountered {} sync error(s): {}",
            errors.len(),
            errors.join("; ")
        )
        .into());
    }

    Ok(())
}
